from datetime import datetime
from datetime import timedelta
from collections import Counter
import math
import json
import numpy as np
import pandas as pd
import src.crawler as c

def preprocess(org, repo):
    add_company_column_for_users(org, repo)
    add_company_column_for_issues(org, repo)

def add_company_column_for_users(org, repo):
    companies = c.get_companies(org, repo)
    users = c.get_issue_authors(org, repo)
    users_with_company = merge_users_with_company(users, companies)
    users_with_company.to_csv(org + "_" + repo + "_" + c.user_file_suffix + "_with_company.csv", sep='\t')

def add_company_column_for_issues(org, repo):
    users = c.get_issue_authors_with_company(org, repo)
    issues = c.get_issues(org, repo)
    issues_with_company = merge_issues_with_company_column(issues, users)
    issues_with_company.to_csv(org + "_" + repo + "_" + c.issue_file_suffix + "with_employer", sep='\t')

def calculate_issue_time_difference(org, repo, issues, timeA, timeB, company_affiliation_based_on_devstats_data=False):
    time_format = "%Y-%m-%d %H:%M:%S"
    issues_with_time_difference = []
    contributors_companies = get_employer_for_contributors(org, repo, company_affiliation_based_on_devstats_data)
    for _, issue in issues.iterrows():
        user = issue.user_login
        employer = contributors_companies.get(user)
        still_open = type(issue[timeB]) is float and math.isnan(issue[timeB]) or type(issue[timeA]) is float and math.isnan(issue[timeA])
        
        if not employer or still_open:
            continue
        if company_affiliation_based_on_devstats_data:
            employer = harmonize_company_name(determine_historic_employer(employer, datetime.strptime(issue[timeA], time_format)))

        issue_dict = {
            "number": issue.number,
            "user_login": user,
            "company": employer,
            timeA: issue[timeA],
            timeB: issue[timeB],
            "time_difference": determine_processing_time(issue[timeA], issue[timeB], time_format, in_seconds=True),
            "title": issue.title,
            "priority": issue.priority,
            "kind": issue.kind
            }
        issues_with_time_difference.append(issue_dict)

    return pd.DataFrame(issues_with_time_difference, columns=list(issue_dict.keys()))

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

def determine_company_for_issues_with_history(issues):
    time_format = "%Y-%m-%d %H:%M:%S"
    issues = issues.dropna(subset=["created_at"])
    contributors_companies = get_employer_for_contributors("", "", based_on_devstats_data=True)
    issues["company"] = issues.apply(lambda issue: harmonize_company_name(determine_historic_employer(contributors_companies.get(issue["user_login"]), datetime.strptime(issue["created_at"], time_format))), axis = 1)
    return issues
    # else:
    #     return pd.merge(issues, users[["user_login", "company"]], how="left", on="user_login")

def merge_issues_with_company_column(issues, users):
        return pd.merge(issues, users[["user_login", "company"]], how="left", on="user_login")

def merge_pulls_with_issue_priority_and_kind(pulls, issues):
    return pd.merge(pulls, issues[["number", "kind", "priority"]], how="left", on="number")

def merge_users_with_company(users, companies):
    users["company"] = users.apply(_determine_employer, companies = companies, axis = 1)
    return users

def merge_issues_with_issue_comments(issues, issue_comments):
    # TODO Note that we only crawled closed issues! We have to take all issues into account
    issue_comments.rename(columns = {'user_login':'commentator', 'created_at':'commented_at', 'issue':'number'}, inplace = True) 
    issues = pd.merge(issue_comments, issues, how='left', on='number')
    # TODO even though merge is left, result contains some comments with nan issue values
    print(issues)
    for _, issue in issues.iterrows():
        if "nan" in str(issue.priority):
            print(issue)
    return issues

def add_column_for_user_contribution_strength(issues):
    user_logins = issues["user_login"].values
    counter = Counter(user_logins)
    issues["user_contributions"] = issues.apply(lambda issue: counter[issue["user_login"]], axis = 1)
    return issues

def add_dummy_column_for_pr_merge_state(pulls):
    pulls["pr_is_merged"] = pulls.apply(_determine_if_merged, axis = 1)
    return pulls
     
def _determine_if_merged(pull):
    if pd.notnull(pull["merged_at"]):
        return 1
    else:
        return 0

def add_dummy_column_for_rounded_year(issues):
    time_format = "%Y-%m-%d %H:%M:%S"
    issues["year"] = issues.apply(lambda issue: round_to_year(datetime.strptime(issue.created_at, time_format)), axis = 1)
    return issues

def add_dummy_column_for_month(issues):
    time_format = "%Y-%m-%d %H:%M:%S"
    issues["month"] = issues.apply(lambda issue: datetime.strptime(issue.created_at, time_format).replace(day=1, hour=0, minute=0, second=0, microsecond=0), axis = 1)
    return issues

def round_to_year(dt):
    dt_half_year = dt.replace(month=7, day=1, hour=0, minute=0, second=0, microsecond=0)
    if dt >= dt_half_year:
        rounded_year = dt.year + 1 # round up
    else:
        rounded_year = dt.year # round down
    return rounded_year
    
def add_dummy_column_for_each_kind(issues):
    issue_kinds = ['failing-test', 'feature', 'cleanup', 'documentation', 'flake', 'api-change', 'design', 'deprecation', 'bug']
    for kind in issue_kinds:
        issues[kind] = issues.apply(_determine_if_kind, kind=kind, axis = 1)
    return issues

def _determine_if_kind(issue, kind):
    if pd.notnull(issue["kind"]) and kind in issue["kind"]:
        return 1
    else:
        return 0

def filter_issues_for_kind(issues, kind):
    issues = issues.dropna(subset=["kind", "company"])
    issues[kind] = issues.apply(_add_dummy_var_for_kind, kind=kind, axis = 1)
    return issues.loc[issues[kind] == True]

def filter_issues_by_time(issues, time, after=True):
    issues = issues.dropna(subset=["created_at", "company"])
    issues["time"] = issues.apply(_add_dummy_var_for_time, time = time, axis = 1)
    return issues.loc[issues["time"] == after]

def filter_pull_requests_from_issues(org, repo, issues):
    pulls = list(c.get_pulls(org, repo).number.values)
    return issues.loc[issues["number"].isin(pulls)]

def _add_dummy_var_for_kind(issue, kind):
    if kind in issue["kind"]:
        return True
    else:
        return False

def _add_dummy_var_for_time(issue, time):
    time_format = "%Y-%m-%d %H:%M:%S"
    if datetime.strptime(issue.created_at, time_format) > time:
        return True
    else:
        return False

def print_company_representation_in_pulls(pulls, companies):
    pulls = determine_company_for_issues_with_history(pulls)
    pulls['company'] = np.where(pulls['company'].isin(companies), pulls['company'], 'unknown')
    df = pd.DataFrame.from_dict(Counter(pulls.company.values), orient='index').sort_values([0])
    df["ratio"] = df[0] / len(pulls.index)
    print(df)
    return df

def get_last_employer_for_contributors(org, repo, based_on_devstats_data=False):
    authors_df = get_users(org, repo, based_on_devstats_data)
    return authors_df.fillna('').set_index('user_login')['company'].to_dict()

def get_employer_for_contributors(org, repo, based_on_devstats_data=False):
    if based_on_devstats_data:
        authors_df = get_formatted_devstats_user()
        return authors_df.fillna('').set_index('user_login')['affiliation'].to_dict()
    else:
        authors_df = c.get_issue_authors_with_company(org, repo)
        return authors_df.fillna('').set_index('user_login')['company'].to_dict()
    
def compare_users_with_devstats_data():
    datastore = c.get_devstats_user()
    df = pd.DataFrame(datastore)
    devstats_users = list(set(df["login"].values))
    users_df = c.get_issue_authors_with_company("kubernetes", "kubernetes")
    users = list(set(users_df["user_login"].values))

    intersection = {user for user in users if user in devstats_users}
    print("# of crawled users: " + str(len(users)))
    print("# of dev stats users: " + str(len(devstats_users)))
    print("# of common users: " + str(len(intersection)))
    print("percentage of users covered by devstats: " + str(len(intersection) / len(users)))
    
    users_df["company"].fillna('unknown', inplace=True)
    users_df_without_company = users_df.loc[users_df["company"] == "unknown"]
    users_without_company = list(set(users_df_without_company["user_login"].values))
    print("# of users without company affiliation: " + str(len(users_without_company)))
    user_w_company_in_devstats = {user for user in users_without_company if user in devstats_users}
    print("# of users with company affiliation in devstats: " + str(len(user_w_company_in_devstats)))

def determine_bots_based_on_devstats_data():
    devstats_users = pd.DataFrame(c.get_devstats_user())
    bots = devstats_users.loc[devstats_users["affiliation"] == "(Robots)"]
    return list(set(bots["login"].values))

def find_bot_comments(org, repo):
    bots = determine_bots_based_on_devstats_data()
    issues = c.get_issues_with_comments(org, repo)
    issues_w_bot_comment = issues.loc[issues["commentator"].isin(bots)]
    print("\nIssues with bot comments:")
    print(issues_w_bot_comment)
    return issues_w_bot_comment

def determine_company_share_of_issues_based_on_devstats_data(org, repo):
    issues = c.get_issues(org, repo)
    users = get_formatted_devstats_user()
    issues_with_company = merge_issues_with_company_column(issues, users)
    return Counter(issues_with_company.company.values)

def compare_company_share_of_issues_with_devstats_data(org, repo):
    company_issue_counter = Counter(c.get_issues_with_company(org, repo).company.values)
    company_issue_counter_devstats = determine_company_share_of_issues_based_on_devstats_data(org, repo)
    print(_filter_by_frequency(company_issue_counter, 500))
    print(_filter_by_frequency(company_issue_counter_devstats, 500))

def get_users(org, repo, based_on_devstats_data):
    if based_on_devstats_data:
        return get_formatted_devstats_user()
    else:
        return c.get_issue_authors_with_company(org, repo)

def get_formatted_devstats_user():
    devstats_users = pd.DataFrame(c.get_devstats_user())
    devstats_users = devstats_users.rename(columns = {"login":"user_login"}).drop_duplicates(subset=["user_login"]).dropna(subset=["affiliation"]) 
    devstats_users["company"] = devstats_users.apply(_determine_last_employer_in_devstats_and_harmonize_format, axis = 1)
    return devstats_users

def determine_company_share_of_contributors_based_on_devstats_data(org, repo):
    devstats_users = get_formatted_devstats_user()
    employee_counter = Counter(devstats_users.last_employer.values)
    print(_filter_by_frequency(employee_counter, 50))

def compare_contributor_company_affiliation_with_devstats_data(org, repo):
    companies = list(c.get_companies(org, repo).keys())
    crawled_users = c.get_issue_authors_with_company(org, repo)
    devstats_users = get_formatted_devstats_user()
    devstats_users.rename(columns = {'company':'last_employer'}, inplace = True) 
    users_with_devstats_info = pd.merge(crawled_users, devstats_users[["user_login", "email", "affiliation", "last_employer"]], how="left", on=["user_login"])

    # users identified overall
    users_identified_with_devstats = users_with_devstats_info.loc[users_with_devstats_info["last_employer"].isin(companies)]
    users_identified = users_with_devstats_info.loc[users_with_devstats_info["company"].isin(companies)]
    print("Overall users: " + str(len(users_with_devstats_info.index)) + "\n-> with identified employers: " + str(len(users_identified.index)) + "\n-> with devstats user affiliaton data identified employers: " + str(len(users_identified_with_devstats.index)) + "\n")

    # users identified in just on dataset (either with crawled  or with devstats information)
    users_not_identified = users_with_devstats_info.loc[users_with_devstats_info["company"].isnull()]
    users_identified_with_devstats = users_not_identified.loc[users_not_identified["last_employer"].isin(companies)]
    print(users_identified_with_devstats[["user_login", "company", "affiliation", "last_employer"]])
    users_not_identified_with_devstats = users_with_devstats_info.loc[~users_with_devstats_info["last_employer"].isin(companies)]
    users_identified = users_not_identified_with_devstats.loc[users_not_identified_with_devstats["company"].notnull()]
    print(users_identified[["user_login", "company", "affiliation", "last_employer"]])

    # users' company affiliation conflicting with devstats data
    users_with_devstats_info = users_with_devstats_info.dropna(subset=["company"]) 
    users_with_devstats_info = users_with_devstats_info.dropna(subset=["last_employer"]) 
    users_with_devstats_info = users_with_devstats_info.loc[users_with_devstats_info["last_employer"].isin(companies)]
    conflicting_users = users_with_devstats_info.loc[users_with_devstats_info["company"] != users_with_devstats_info["last_employer"]]
    print(conflicting_users[["user_login", "company", "affiliation", "last_employer"]])
     
def _determine_last_employer_in_devstats_and_harmonize_format(user):  
    if user["affiliation"] == "?":
        return  
    last_employer = user["affiliation"].rsplit(',', 1)[-1].strip()
    return harmonize_company_name(last_employer)

def determine_historic_employer(employer_history, time):
    time_format = "%Y-%m-%d"
    if not employer_history:
        return ""
    historic_employers = employer_history.split("<")
    if not historic_employers:
        return "unkown_employer"
    historic_employers.reverse()
    for employer_info in historic_employers:
        if ',' not in employer_info:
            return employer_info.strip()
        since, employer = employer_info.rsplit(",", 1)
        try:
            if time > datetime.strptime(since.strip(), time_format):
                return employer.strip()
        except ValueError:
            print("### Couldn't parse employment period data for: " + str(employer_info) + " (probably company name contains a comma: " + str(since) + ")")
            return employer_info.strip()
    raise Exception("Error parsing historic employment data for: " + str(employer_info))

def harmonize_company_name(name):
    if name == "Red Hat":
        name = "RedHat"
    return name

def determine_issues_not_being_respected_by_response_time_analysis(org, repo):
    diff = set(c.get_issues_with_response_time(org, repo).number.values).symmetric_difference(set(c.get_issues_with_processing_time(org, repo).number.values))
    print("# of issues not being respected by response time analysis (e.g. due to missing or solely bot comments): " + str(len(diff)))

def find_time_unregularities_in_issues(issues):
    time_format = "%Y-%m-%d %H:%M:%S"
    issues = issues.dropna(subset=["updated_at"])
    updated_at_list = list(issues.updated_at.values)
    updated_at_list.sort()
    last = datetime.fromtimestamp(0)

    for issue in updated_at_list:
        current = datetime.strptime(issue, time_format)
        if (current - last) > timedelta(hours=24):
            print("### unregularity: ###")
            print(last)
            print(current)
        last = current

def verify_data_consistency_for_crawled_issues_and_comments_by_checking_coherent_company_representation(org, repo):
    print("\nCompany representation in issues, in issues with processing time and in issues with comments and reponse time:")
    print(pd.DataFrame.from_dict(_filter_by_frequency(Counter(c.get_issues_with_company(org, repo).company.values), 1000)))
    print(pd.DataFrame.from_dict(_filter_by_frequency(Counter(c.get_issues_with_processing_time(org, repo).company.values), 1000)))
    print(pd.DataFrame.from_dict(_filter_by_frequency(Counter(c.get_issues_with_response_time(org, repo).company.values), 1000)))

    print("\nContributor company affiliation baseline:")
    print(pd.DataFrame.from_dict(_filter_by_frequency(Counter(c.get_issue_authors_with_company(org, repo).company.values), 50)))
    print("\nContributor company affiliation distribution in issues, in issues with processing time and in issues with comments and reponse time:")
    print(pd.DataFrame.from_dict(_filter_by_frequency(Counter(c.get_issues_with_company(org, repo)[["user_login" ,"company"]].drop_duplicates(subset=["user_login"]).company.values), 50)))
    print(pd.DataFrame.from_dict(_filter_by_frequency(Counter(c.get_issues_with_processing_time(org, repo)[["user_login" ,"company"]].drop_duplicates(subset=["user_login"]).company.values), 50)))
    print(pd.DataFrame.from_dict(_filter_by_frequency(Counter(c.get_issues_with_response_time(org, repo)[["user_login" ,"company"]].drop_duplicates(subset=["user_login"]).company.values), 50)))

def _determine_employer(user, companies):
    user_orgs = user["user_orgs"]
    user_company = user["user_company"]
    user_mail = user["user_mail"]

    for company in companies.keys():
        if user_company in companies[company]["companies"] or user_mail in companies[company]["mail_addresses"]:
            return company
    print("##### Company could not be identified.")

def _filter_by_frequency(counter, frequency_level):
    filtered_counter = {x : counter[x] for x in counter if counter[x] >= frequency_level}
    return pd.DataFrame.from_dict(filtered_counter, orient='index').sort_values(by=counter[1])

########################## DEPRECATED BELOW ################################

contributors = {}

def _extract_employer(user_name, users_df):
    company_row = users_df[users_df.user_login == user_name]["company"]
    return company_row.iloc[0] if not company_row.empty else None

def get_employer_from_csv(user_name, org = "kubernetes", repo = "kubernetes"):
    global contributors
    if not contributors:
        contributors_companies = get_last_employer_for_contributors(org, repo)
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