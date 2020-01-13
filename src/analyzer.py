from github import Github
import datetime
import math
import pandas as pd
import src.crawler as c
import src.preprocesser as p

def calculate_issue_processing_time(org, repo):
    issues = c.get_issues(org, repo)
    issues_w_processing_time = p.calculate_issue_time_difference(org, repo, issues, "created_at", "closed_at")
    issues_w_processing_time.rename(columns = {'time_difference':'processing_time'}, inplace = True) 
    issues_w_processing_time.to_csv(org + "_" + repo + "_" + c.issue_file_suffix + "_with_processing_time_5", sep='\t')

def calculate_issue_reponse_time(org, repo):
    issues_w_comments = c.get_issues_with_comments(org, repo)

    if issues_w_comments.empty:
        issues = c.get_issues(org, repo)
        issue_comments = c.get_issue_comments(org, repo)
        issue_comments = p.extract_first_comment_per_issue(issue_comments)
        issues_w_comments = p.merge_issues_with_issue_comments(issues, issue_comments)

    issues_w_response_time_df = p.calculate_issue_time_difference(org, repo, issues_w_comments, "created_at", "commented_at")
    issues_w_response_time_df.rename(columns = {'time_difference':'response_time'}, inplace = True) 
    issues_w_response_time_df.to_csv(org + "_" + repo + "_" + c.issue_file_suffix + "_with_response_time_3", sep='\t')

def calculate_pr_acceptance_rate_by_companies(org, repo):
    companies = _calculate_pr_composition_by_companies(org, repo)
    pr_acceptance_rate = {}
    for company in companies.keys():
        closed_pulls_count = len(companies[company]["closed_pulls"])
        merged_pulls_count = len(companies[company]["merged_pulls"])
        total_pulls_count = merged_pulls_count + closed_pulls_count
        companies[company]["pr_acceptance_rate"] = merged_pulls_count / total_pulls_count if total_pulls_count else 0
        pr_acceptance_rate[company] = merged_pulls_count / total_pulls_count if total_pulls_count else 0
    return pr_acceptance_rate
    
def calculate_avg_issue_response_time_by_company(org, repo):
    issues = c.get_issues(org, repo)
    issue_comments = c.get_issue_comments(org, repo)
    issues_w_comments = p.merge_issues_with_issue_comments(issues, issue_comments)

    companies = c.get_companies(org, repo)
    for employer in companies.keys():
        companies[employer]["response_time"] = datetime.timedelta(0)
        companies[employer]["issue_count"] = 0

    time_format = "%Y-%m-%d %H:%M:%S"
    for index, issue in issues_w_comments.iterrows():
        employer = p.get_employer(issue.user_login_x, org, repo) # TODO make more generic / change merge behavior
        open_issue = type(issue.created_at_y) is float and math.isnan(issue.created_at_y)
        if employer is None or open_issue:
            continue
        companies[employer]["response_time"] = companies[employer]["response_time"] + p.determine_processing_time(issue.created_at_y, issue.created_at_x, time_format) # TODO make more generic / change merge behavior
        companies[employer]["issue_count"] += 1

    for employer in companies.keys():
        companies[employer]["avg_response_time"] = companies[employer]["response_time"].total_seconds() / companies[employer]["issue_count"] if companies[employer]["issue_count"] else 0
        print(str(employer) + " - avg_response_time: " + str(companies[employer]["avg_response_time"]))
        print(str(employer) + " - issue_count: " + str(companies[employer]["issue_count"]))

    return companies

def calculate_avg_issue_processing_time_by_company(org, repo):
    # note that PRs are included here as they are a special type of an issue
    time_format = "%Y-%m-%d %H:%M:%S"
    companies = c.get_companies(org, repo)
    for employer in companies.keys():
        companies[employer]["processing_time"] = datetime.timedelta(0)
        companies[employer]["issue_count"] = 0

    issues = c.get_issues(org, repo)

    print("Start iterating over issues...")
    for index, issue in issues.iterrows():
        employer = p.get_employer(issue.user_login, org, repo)
        still_open = type(issue.closed_at) is float and math.isnan(issue.closed_at)
        if employer is None or still_open:
            continue

        print(issue.title)
        companies[employer]["processing_time"] = companies[employer]["processing_time"] + p.determine_processing_time(issue.created_at, issue.closed_at, time_format) 
        companies[employer]["issue_count"] += 1

    for employer in companies.keys():
        companies[employer]["avg_processing_time"] = companies[employer]["processing_time"].total_seconds() / companies[employer]["issue_count"] if companies[employer]["issue_count"] else 0
        print(str(employer) + " - avg_processing_time: " + str(companies[employer]["avg_processing_time"]))
        print(str(employer) + " - issue_count: " + str(companies[employer]["issue_count"]))

    return companies

def calculate_issue_kind_share_by_company(org, repo):
    issue_kinds = ['failing-test', 'feature', 'cleanup', 'documentation', 'flake', 'api-change', 'design', 'deprecation', 'bug']
    companies = c.get_companies(org, repo).keys()
    distribution = {key: dict.fromkeys(issue_kinds, 0) for key in companies}
    issues = c.get_issues_with_company(org, repo).dropna(subset=["kind", "company"]) 
    for index, issue in issues.iterrows():
        kinds = issue.kind.split(",")
        for kind in kinds:
            distribution[issue.company][kind] += 1 / len(kinds)
    return pd.DataFrame(distribution)

###################################### PRIVATE ############################################

def _calculate_pr_composition_by_companies(org, repo):
    # only consider 'closed' PRs for our analysis
    pulls = c.get_pulls(org, repo)
    companies = c.get_companies(org, repo)
    contributors_companies = p.get_companies_for_contributors(org, repo)
    for index, pull in pulls.iterrows():
        user = pull["user_login"]
        employer = contributors_companies.get(user)
        if not employer:
            continue
        merged_at = pull["merged_at"]
        is_merged = not (type(merged_at) is float and math.isnan(merged_at))

        if is_merged:
            companies[employer]["merged_pulls"].append(pull["number"])
        else:
            companies[employer]["closed_pulls"].append(pull["number"])
     
    return companies
        
########################## EMPLOYMENT STATUS: VOLUNTEER VS. EMPLOYEE ################################

with open('github-token', 'r') as token_file:
    token = token_file.read().rstrip("\n")

g = Github(token)
contributors = {}

def calculate_issue_processing_time_for_org_by_employment_status(org):
    # note that PRs are included here as they are a special type of an issue
    # WARNING: apparently only works if one is part of that specific org
    
    # timedelta is needed for time calculation because issue.closed_at
    # returns datetime object with '%Y-%m-%dT%H:%M:%SZ format'
    employee_processing_time = datetime.timedelta(0)
    volunteer_processing_time = datetime.timedelta(0)
    employee_issue_count = 0
    volunteer_issue_count = 0

    i = 0
    issues = c.get_issues_for_org(org)
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

def calculate_pr_acceptance_rate_by_employment_status(org, repo = None):
    if repo is not None:
        repo_objects = [g.get_repo(org + "/" + repo)]
    else:
        repo_objects = g.get_organization(org).get_repos(type="public")

    volunteer_merged_pulls = 0
    volunteer_closed_pulls = 0
    employee_merged_pulls = 0   
    employee_closed_pulls = 0

    for repo_object in repo_objects:
        pr_composition = _calculate_pr_composition_for_repo_by_employment_status(repo_object, org)
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

def _calculate_pr_composition_for_repo_by_employment_status(repo_object, org):
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
    user_mail = c.extract_mail_domain(user.email)

    # len(S1.intersection(S2)) > 0
    if user_company in companies or user_mail in mail_addresses or any(x in user_orgs for x in orgs):
        contributors[user_login_name] = True
    else: 
        contributors[user_login_name] = False

################################ DEPRECATED BELOW ######################################

def get_company_orgs_for_org(org):
    # dummy implementation for testing only
    return ["sap-cloudfoundry", "SAP", "pivotal-cf", "pivotal", "SUSE"]

def get_company_mail_addresses_for_org(org):
    # dummy implementation for testing only
    return ["@sap.com"]

def get_companies_for_org(org):
    # dummy implementation for testing only
    return ["SAP SE", "@SAP", "@Pivotal", "SAP", "@SUSE"]
