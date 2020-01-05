from github import Github
from collections import Counter
import re
import datetime
import pandas as pd
import math
import src.crawler as c

with open('github-token', 'r') as token_file:
    token = token_file.read().rstrip("\n")
 
g = Github(token)
contributors = {}

def get_orgs_for_repo(repo):
    mail_domains = []
    orgs = []
    companys = []

    for contributor in g.get_repo(repo).get_contributors():
        for org in contributor.get_orgs():
            orgs.append(org.login)
        companys.append(contributor.company)
        mail = contributor.email or ""
        mail_domain = re.search("@[\w.]+", mail)
        if mail_domain is not None:
            mail_domains.append(mail_domain.group())
        
    print(mail_domains)
    print(orgs)
    print(companys)

    print(Counter(mail_domains))
    print(Counter(orgs))
    print(Counter(companys))

def extract_mail_domain(mail_address):
    if mail_address is None:
        return
    mail_domain = re.search("@[\w.]+", mail_address)
    if mail_domain is None:
        return
    else:
        return mail_domain.group()

def calculate_issue_processing_time(org, repo):
    print("##########")
    time_format = "%Y-%m-%d %H:%M:%S"
    issues = c.get_issues(org, repo)
    issues_with_processing_time = []
    for index, issue in issues.iterrows():
        user = issue.user_login
        employer = get_employer(user, org, repo)
        still_open = type(issue.closed_at) is float and math.isnan(issue.closed_at)
        if employer is None or still_open:
            continue

        issue_dict = {
            "number": issue.number,
            "user_login": user,
            "company": employer,
            "created_at": issue.created_at,
            "closed_at": issue.closed_at,
            "processing_time": _determine_processing_time(issue.created_at, issue.closed_at, time_format),
            "title": issue.title
            }
        issues_with_processing_time.append(issue_dict)

    issues_df = pd.DataFrame(issues_with_processing_time, columns=["number", "user_login", "company", "created_at", "closed_at", "processing_time", "title"])
    issues_df.to_csv(org + "_" + repo + "_" + c.issue_file_suffix + "_with_processing_time", sep='\t')

def _determine_processing_time(start_time, end_time, time_format):
    return (datetime.datetime.strptime(end_time, time_format) - datetime.datetime.strptime(start_time, time_format)).total_seconds()

def get_employer(contributor, org, repo):
    if contributor not in contributors:
        _determine_employer(contributor, org, repo)
    return contributors.get(contributor)

def _determine_employer(user_login_name, org, repo):
    pull_authors = c.get_pull_authors(org, repo)
    # pull_authors.set_index("user_login")
    user = pull_authors.loc[pull_authors["user_login"] == user_login_name]
    if user.empty:
        return
    # user["user_mail"].item() and user["user_company"].item() are 'nan' if not specified
    user_orgs = user["user_orgs"]
    user_company = user["user_company"].item()
    user_mail = user["user_mail"].item()
    
    companies = c.get_companies(org, repo)
    for company in companies.keys():
        if user_company in companies[company]["companies"] or user_mail in companies[company]["mail_addresses"]:
            contributors[user_login_name] = company