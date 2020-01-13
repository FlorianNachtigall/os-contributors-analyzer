from datetime import datetime
import math
import json
import pandas as pd
import src.crawler as c

def preprocess(org, repo):
    add_company_column_for_users(org, repo)
    add_company_column_for_issues(org, repo)
    
def add_company_column_for_users(org, repo):
    companies = c.get_companies(org, repo)
    users = c.get_issue_authors(org, repo)
    users_with_company = merge_users_with_company(users, companies)
    users_with_company.to_csv(org + "_" + repo + "_" + c.user_file_suffix + "_with_company", sep='\t')

def add_company_column_for_issues(org, repo):
    users = c.get_issue_authors_with_company(org, repo)
    issues = c.get_issues(org, repo)
    issues_with_company = merge_issues_with_company_column(issues, users)
    issues_with_company.to_csv(org + "_" + repo + "_" + c.issue_file_suffix + "with_employer", sep='\t')

def calculate_issue_time_difference(org, repo, issues, timeA, timeB):
    time_format = "%Y-%m-%d %H:%M:%S"
    issues_with_time_difference = []
    contributors_companies = get_companies_for_contributors(org, repo)
    for index, issue in issues.iterrows():
        user = issue.user_login
        employer = contributors_companies.get(user)
        still_open = type(issue[timeB]) is float and math.isnan(issue[timeB]) or type(issue[timeA]) is float and math.isnan(issue[timeA])
        if not employer or still_open:
            continue

        issue_dict = {
            "number": issue.number,
            "user_login": user,
            "company": employer,
            timeA: issue[timeA],
            timeB: issue[timeB],
            "time_difference": determine_processing_time(issue[timeA], issue[timeB], time_format, in_seconds=True),
            "title": issue.title
            }
        issues_with_time_difference.append(issue_dict)

    return pd.DataFrame(issues_with_time_difference, columns=["number", "user_login", "company", timeA, timeB, "time_difference", "title"])

def extract_first_comment_per_issue(issue_comments):
    time_format = "%Y-%m-%d %H:%M:%S"
    first_issue_comments = {}
    for index, comment in issue_comments.iterrows():
        earliest_comment = first_issue_comments.get(comment.issue)
        comment_time = datetime.strptime(comment.created_at, time_format)
        if earliest_comment is None:
            first_issue_comments[comment.issue] = (index, comment_time)
        elif comment_time < earliest_comment[1]:
            first_issue_comments[comment.issue] = (index, comment_time)
            issue_comments.drop(earliest_comment[0], inplace=True)
        else:
            issue_comments.drop(index, inplace=True)
    return issue_comments

def determine_processing_time(start_time, end_time, time_format, in_seconds = False):
    time_diff = datetime.strptime(end_time, time_format) - datetime.strptime(start_time, time_format)
    return time_diff.total_seconds() if in_seconds else time_diff

def merge_issues_with_company_column(issues, users):
    return pd.merge(issues, users[["user_login", "company"]], how="left", on="user_login")

def merge_users_with_company(users, companies):
    users["company"] = users.apply(_determine_employer, companies = companies, axis = 1)
    return users

def merge_issues_with_issue_comments(issues, issue_comments):
    # TODO Note that we only crawled closed issues! We have to take all issues into account
    issue_comments.rename(columns = {'user_login':'commentator', 'created_at':'commented_at', 'issue':'number'}, inplace = True) 
    issues = pd.merge(issue_comments, issues, how='left', on='number')
    # TODO even though merge is left, result contains some comments with nan issue values
    print(issues)
    for index, issue in issues.iterrows():
        if "nan" in str(issue.priority):
            print(issue)
    return issues

def get_companies_for_contributors(org, repo):
    authors_df = c.get_issue_authors_with_company(org, repo).fillna('')
    return authors_df.set_index('user_login')['company'].to_dict()

def compare_users_with_devstats_data(devstats_filename):
    with open(devstats_filename, 'r') as f:
        datastore = json.load(f)
        df = pd.DataFrame(datastore)
        devstats_users = list(set(df["login"].values))
        users_df = c.get_issue_authors("kubernetes", "kubernetes")
        users = list(set(users_df["user_login"].values))

        intersection = {user for user in users if user in devstats_users}
        print(intersection)
        print("# of users: " + str(len(users)))
        print("# of common users: " + str(len(intersection)))
        print("percentage of users covered by devstats: " + str(len(intersection) / len(users)))

def _determine_employer(user, companies):
    user_orgs = user["user_orgs"]
    user_company = user["user_company"]
    user_mail = user["user_mail"]

    for company in companies.keys():
        if user_company in companies[company]["companies"] or user_mail in companies[company]["mail_addresses"]:
            return company
    print("##### Company could not be identified.")

########################## DEPRECATED BELOW ################################

contributors = {}

def _extract_employer(user_name, users_df):
    company_row = users_df[users_df.user_login == user_name]["company"]
    return company_row.iloc[0] if not company_row.empty else None

def get_employer_from_csv(user_name, org = "kubernetes", repo = "kubernetes"):
    global contributors
    if not contributors:
        contributors_companies = get_companies_for_contributors(org, repo)
    return contributors_companies.get(user_name)

def get_employer(user_name, org = "kubernetes", repo = "kubernetes"):
    global contributors
    if user_name not in contributors:
        issue_authors = c.get_issue_authors(org, repo)
        user = issue_authors[issue_authors["user_login"] == user_name]
        if user.empty:
            return
        companies = c.get_companies(org, repo)
        contributors[user_name] = _determine_employer(user.iloc[0], companies)
    return contributors[user_name]