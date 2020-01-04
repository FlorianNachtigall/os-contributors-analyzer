from github import Github
from collections import Counter
import datetime
import re
import math
import json
import src.crawler as c

with open('github-token', 'r') as token_file:
    token = token_file.read().rstrip("\n")

with open('companies.json', 'r') as f:
    companies = json.load(f)

g = Github(token)
contributors = {}
pull_authors = c.get_pull_authors("kubernetes" , "kubernetes")
# pull_authors.set_index("user_login")

def get_repos_for_org(org):
    repos = []
    for repo in g.get_organization("cloudfoundry").get_repos(type="public"):
        repos.append(repo.name)
    return repos

def get_issues_for_org(org):
    return g.get_organization(org).get_issues(filter="all", state="closed")

def calculate_issue_processing_time(org):
    # note that PRs are included here as they are a special type of an issue
    # WARNING: apparently only works if one is part of that specific org
    
    # timedelta is needed for time calculation because issue.closed_at
    # returns datetime object with '%Y-%m-%dT%H:%M:%SZ format'
    employee_processing_time = datetime.timedelta(0)
    volunteer_processing_time = datetime.timedelta(0)
    employee_issue_count = 0
    volunteer_issue_count = 0

    i = 0
    issues = get_issues_for_org(org)
    print("Start iterating over issues...")
    for issue in issues:
        print(issue.title)
        i += 1
        if i >= 10:
            break
        user = issue.user.login
        if _is_employee(user, org):
            employee_issue_count += 1
            employee_processing_time = employee_processing_time + (issue.closed_at - issue.created_at)
        else:
            volunteer_issue_count += 1
            volunteer_processing_time = volunteer_processing_time + (issue.closed_at - issue.created_at)
          
    print("# of volunteer issues: " + str(volunteer_issue_count))
    print("# of employee issues: " + str(employee_issue_count))
    volunteer_avg_time = volunteer_processing_time.total_seconds() / volunteer_issue_count
    employee_avg_time = employee_processing_time.total_seconds() / employee_issue_count
    return volunteer_avg_time, employee_avg_time

def get_company_orgs_for_org(org):
    # dummy implementation for testing only
    return ["sap-cloudfoundry", "SAP", "pivotal-cf", "pivotal", "SUSE"]

def get_company_mail_addresses_for_org(org):
    # dummy implementation for testing only
    return ["@sap.com"]

def get_companies_for_org(org):
    # dummy implementation for testing only
    return ["SAP SE", "@SAP", "@Pivotal", "SAP", "@SUSE"]

def calculate_pr_acceptance_rate(org, repo = None):
    if repo is not None:
        repo_objects = [g.get_repo(org + "/" + repo)]
    else:
        repo_objects = g.get_organization(org).get_repos(type="public")

    volunteer_merged_pulls = 0
    volunteer_closed_pulls = 0
    employee_merged_pulls = 0   
    employee_closed_pulls = 0

    for repo_object in repo_objects:
        pr_composition = _calculate_pr_composition_for_repo(repo_object, org)
        volunteer_merged_pulls += pr_composition[0] 
        volunteer_closed_pulls += pr_composition[1]
        employee_merged_pulls += pr_composition[2]    
        employee_closed_pulls += pr_composition[3] 

    volunteer_pulls_count = volunteer_closed_pulls + volunteer_merged_pulls
    employee_pulls_count = employee_closed_pulls + employee_merged_pulls

    print("###contributors")
    print(contributors)
    print(volunteer_closed_pulls)
    print(employee_closed_pulls)
    print(volunteer_merged_pulls)
    print(employee_merged_pulls)
    if employee_pulls_count is 0 or volunteer_pulls_count is 0:
        # TODO instead of returning 0,0 raise exception so that pr acceptance rate in overall org does not get influenced
        return 0, 0

    volunteer_acceptance_rate = volunteer_merged_pulls / volunteer_pulls_count
    employee_acceptance_rate = employee_merged_pulls / employee_pulls_count

    return volunteer_acceptance_rate, employee_acceptance_rate

def calculate_pr_acceptance_rate_by_companies(org, repo = None):

    companies = _calculate_pr_composition_by_companies(repo, org)
    pr_acceptance_rate = {}
    for company in companies.keys():
        closed_pulls_count = len(companies[company]["closed_pulls"])
        merged_pulls_count = len(companies[company]["merged_pulls"])
        total_pulls_count = merged_pulls_count + closed_pulls_count
        companies[company]["pr_acceptance_rate"] = merged_pulls_count / total_pulls_count if total_pulls_count else 0
        pr_acceptance_rate[company] = merged_pulls_count / total_pulls_count if total_pulls_count else 0
    return pr_acceptance_rate

def _calculate_pr_composition_for_repo(repo_object, org):
    volunteer_closed_pulls = []
    volunteer_merged_pulls = []
    employee_closed_pulls = []
    employee_merged_pulls = []
    
    # only consider 'closed' PRs for our analysis
    for pull in repo_object.get_pulls(state="closed"):      
        user = pull.user.login
        print(pull.state)
        print(user)
        if _is_employee(user, org):
            if pull.merged_at is not None:
                employee_merged_pulls.append(pull)
            else:
                employee_closed_pulls.append(pull)
        else:
            if pull.merged_at is not None:
                volunteer_merged_pulls.append(pull)
            else:
                volunteer_closed_pulls.append(pull)
    return (len(volunteer_merged_pulls), len(volunteer_closed_pulls), len(employee_merged_pulls), len(employee_closed_pulls))

def _calculate_pr_composition_for_repo_df(repo, org):
    volunteer_closed_pulls = []
    volunteer_merged_pulls = []
    employee_closed_pulls = []
    employee_merged_pulls = []
    
    # only consider 'closed' PRs for our analysis
    pulls = c.get_pulls(org, repo)
    for index, pull in pulls.iterrows():
        user = pull["user_login"]
        employer = _is_employee(user, org)
        merged_at = pull["merged_at"]
        is_merged = not (type(merged_at) is float and math.isnan(merged_at))
        if employer == "Google":
            if is_merged:
                employee_merged_pulls.append(pull["number"])
            else:
                employee_closed_pulls.append(pull["number"])
        elif employer == "RedHat":
            if is_merged:
                volunteer_merged_pulls.append(pull["number"])
            else:
                volunteer_closed_pulls.append(pull["number"])
    return (len(volunteer_merged_pulls), len(volunteer_closed_pulls), len(employee_merged_pulls), len(employee_closed_pulls))

def _calculate_pr_composition_by_companies(repo, org):
    # only consider 'closed' PRs for our analysis
    pulls = c.get_pulls(org, repo)
    for index, pull in pulls.iterrows():
        user = pull["user_login"]
        employer = _is_employee(user, org)
        if employer is None:
            continue
        merged_at = pull["merged_at"]
        is_merged = not (type(merged_at) is float and math.isnan(merged_at))

        if is_merged:
            companies[employer]["merged_pulls"].append(pull["number"])
        else:
            companies[employer]["closed_pulls"].append(pull["number"])
     
    return companies

def _extract_mail_domain(mail_address):
    if mail_address is None:
        return
    mail_domain = re.search("@[\w.]+", mail_address)
    if mail_domain is None:
        return
    else:
        return mail_domain.group()
        
def _is_employee(contributor, org):
    if contributor not in contributors:
        _determine_is_employee(contributor, org)
    return contributors.get(contributor)

def _determine_is_employee(user_login_name, org):
    orgs = get_company_orgs_for_org(org)
    companies = get_companies_for_org(org)
    mail_addresses = get_company_mail_addresses_for_org(org)

    user = g.get_user(user_login_name)
    user_orgs = []
    for user_org in user.get_orgs():
        user_orgs.append(user_org.login)
    user_company = user.company
    user_mail = _extract_mail_domain(user.email)

    # len(S1.intersection(S2)) > 0
    if user_company in companies or user_mail in mail_addresses or any(x in user_orgs for x in orgs):
        contributors[user_login_name] = True
    else: 
        contributors[user_login_name] = False

def _get_employer(contributor, org):
    if contributor not in contributors:
        _determine_employer(contributor)
    return contributors.get(contributor)

def _determine_employer(user_login_name):
    user = pull_authors.loc[pull_authors["user_login"] == user_login_name]
    if user.empty:
        return
    # user["user_mail"].item() and user["user_company"].item() are 'nan' if not specified
    user_orgs = user["user_orgs"]
    user_company = user["user_company"].item()
    user_mail = user["user_mail"].item()

    for company in companies.keys():
        if user_company in companies[company]["companies"] or user_mail in companies[company]["mail_addresses"]:
            contributors[user_login_name] = company