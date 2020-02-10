from datetime import datetime
from scipy import stats
from collections import Counter 
import statsmodels.formula.api as sm
import matplotlib.pyplot as plt
import pandas as pd
import src.analyzer as a
import src.crawler as c
import src.preprocesser as p
import src.visualizer as v
import src.statistics as s

org = "kubernetes"
repo = "kubernetes"

###########################################################################
############################## CRAWLING ###################################
# c.crawl(org, repo)
# p.preprocess(org, repo)
# c.crawl_issues_with_comments(org, repo)


###########################################################################
####################### ANALYSIS & VISUALIZATION ##########################

# metric #1: PR accepteance rate
# print(a.calculate_pr_acceptance_rate_by_companies(org, repo))

# # metric #2: processing time
# a.calculate_issue_processing_time(org, repo)
# issues = c.get_issues_with_processing_time(org, repo)
# issues = v.filter_issues_after(issues, datetime(2017, 1, 1))
# v.boxplot_issue_processing_time(issues)

# # metric #3: response time
# a.calculate_issue_reponse_time(org, repo)
# issues = c.get_issues_with_response_time(org, repo)
# v.boxplot_issue_reponse_time(issues)

# # metric #4: prioritization
# issue_kind_distribution = a.calculate_issue_kind_share_by_company(org, repo)
# v.show_stacked_bar_chart_for_issue_kinds_by_company(issue_kind_distribution)

# # other metrics
# # print(a.calculate_avg_issue_response_time_by_company(org, repo))
# # print(a.calculate_avg_issue_processing_time_by_company(org, repo))

# data inspection / controlling variables
# issues = c.get_issues_with_company(org, repo)
# issues = p.filter_issues_for_kind(issues, "bug")
# v.show_stacked_bar_chart_for_issue_priorities_by_company(issues)

###########################################################################
######################### STATISTICAL ANALYSIS ############################
# print(Counter(p.get_companies_for_contributors(org, repo).values()))
# s.print_ols_test_for_issue_processing_time(org, repo)
# s.print_ols_test_for_pr_acceptance_rate(org, repo)
# s.print_ols_test_for_issue_prioritization(org, repo)

###########################################################################
############################ DATA INSIGHTS ################################
# p.compare_contributor_company_affiliation_with_devstats_data(org, repo)
# p.compare_company_share_of_issues_with_devstats_data(org, repo)
# p.compare_users_with_devstats_data()
print(p.get_companies_for_contributors_based_on_devstats_data(org, repo))

###########################################################################
############################# PLAYGROUND ##################################
# p.determine_bots_based_on_devstats_data()
# p.find_bot_comments(org, repo)
# print(pd.DataFrame.from_dict(Counter(issues.company.values), orient='index'))
# issues = issues.dropna(subset=["company"])
# issues_google = issues.loc[issues["company"] == "Google"]
# issues_not_google = issues.loc[issues["company"] != "Google"]
# v.boxplot_issue_processing_time(issues_google)
# v.boxplot_issue_processing_time(issues_not_google)
# issues_google = v._remove_outliers(issues_google, "processing_time")
# issues_not_google = v._remove_outliers(issues_not_google, "processing_time")
# v.boxplot_issue_processing_time(issues_google)
# v.boxplot_issue_processing_time(issues_not_google)

# t2, p2 = stats.ttest_ind(issues_google["processing_time"], issues_not_google["processing_time"])
# print(t2)
# print(p2)

# issues = v.filter_issues_for_kind(issues, "bug")
# issues = issues.loc[issues["priority"] < 3]
# v.boxplot_issue_processing_time(issues)

# commentators = set(c.get_issues_with_comments(org, repo)["user_login"].values)
# for co in commentators:
#     if "bot" in co:
#         print(co)

plt.show()