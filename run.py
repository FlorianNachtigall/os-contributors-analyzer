from datetime import datetime
from scipy import stats
from collections import Counter 
import statsmodels.formula.api as sm
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import src.analyzer as a
import src.crawler as c
import src.preprocesser as p
import src.visualizer as v
import src.statistics as s

# org, repo = "istio", "istio"
org, repo = "kubernetes", "kubernetes"
based_on_devstats_data = True
###########################################################################
############################## CRAWLING ###################################
# c.crawl(org, repo)
# p.preprocess(org, repo)
# c.crawl_issues_with_comments(org, repo)
# companies = list(c.get_companies(org, repo).keys())
companies = ['Google', 'Huawei', 'Microsoft', 'RedHat', 'VMware', 'ZTE', 'Fujitsu', 'IBM']

###########################################################################
####################### ANALYSIS & VISUALIZATION ##########################

# metric #1: PR accepteance rate
pulls = c.get_pulls(org, repo)

# overall_ac_rate = a.calculate_overall_pr_acceptance_rate(org, repo)
# ac_rate = a.calculate_pr_acceptance_rate_by_companies(org, repo, based_on_devstats_data)
# v.show_bar_chart_for_pr_rejection_rates_by_company(ac_rate, overall_ac_rate)
# v.show_area_chart_for_pr_rejection_rates_over_time(pulls)
# v.show_line_chart_for_pr_rejection_rates_over_time(pulls, based_on_devstats_data, companies)

# metric #2: processing time
# a.calculate_issue_processing_time(org, repo)
issues_with_processing_time = c.get_issues_with_processing_time(org, repo, based_on_devstats_data)    
issues_with_processing_time = p.filter_pull_requests_from_issues(org, repo, issues_with_processing_time)
s.print_descriptive_metrics(issues_with_processing_time, "processing_time", companies)
v.boxplot_issue_processing_time(issues_with_processing_time, companies)

# metric #3: response time
# a.calculate_issue_reponse_time(org, repo)
issues_with_response_time = c.get_issues_with_response_time(org, repo, based_on_devstats_data)
issues_with_response_time = p.filter_pull_requests_from_issues(org, repo, issues_with_response_time)
s.print_descriptive_metrics(issues_with_response_time, "response_time", companies)
v.boxplot_issue_reponse_time(issues_with_response_time, companies)

# other metrics
# print(a.calculate_avg_issue_response_time_by_company(org, repo))
# print(a.calculate_avg_issue_processing_time_by_company(org, repo))
# s.calculate_similarity_between_issue_response_and_processing_time(org, repo, based_on_devstats_data, companies)
# v.show_line_chart_for_metrics_over_time(pulls, issues_with_processing_time, issues_with_response_time, companies)
# data inspection / controlling variables
# if based_on_devstats_data:
#     issues = c.get_issues(org,repo)
#     issues = p.determine_company_for_issues_with_history(issues)
# else:
#     issues = c.get_issues_with_company(org, repo)

# issues = p.filter_pull_requests_from_issues(org, repo, issues)
# issue_kind_distribution = a.calculate_issue_kind_share_by_company(issues, companies)
# v.show_stacked_bar_chart_for_issue_kinds_by_company(issue_kind_distribution)
# v.show_stacked_bar_chart_for_issue_priorities_by_company(issues, companies)
# v.show_stacked_area_chart_for_company_issues_over_time(issues, companies)
# v.show_stacked_area_chart_for_issue_contributor_affiliation_over_time(issues, companies)

###########################################################################
######################### STATISTICAL ANALYSIS ############################
# s.factor_analysis(org, repo)
# s.print_ols_regression_for_issue_processing_time(org, repo, based_on_devstats_data)
# s.print_ols_regression_for_issue_response_time(org, repo, based_on_devstats_data)
# s.print_logistic_regression_for_pr_acceptance_rate(org, repo, based_on_devstats_data)

###########################################################################
############################ DATA INSIGHTS ################################

p.print_company_representation_in_pulls(pulls, companies)
# p.compare_contributor_company_affiliation_with_devstats_data(org, repo)
# p.compare_company_share_of_issues_with_devstats_data(org, repo)
# p.compare_users_with_devstats_data()

###########################################################################
######################## DATA CONSISTENCY CHECK ###########################
# p.find_bot_comments(org, repo)
# p.verify_data_consistency_for_crawled_issues_and_comments_by_checking_coherent_company_representation(org, repo)
# p.determine_issues_not_being_respected_by_response_time_analysis(org, repo)
# p.find_time_unregularities_in_issues(c.get_issues_with_comments(org, repo))

###########################################################################
############################# PLAYGROUND ##################################

# print(pd.DataFrame.from_dict(Counter(c.get_issues_with_comments(org, repo).priority.values), orient='index'))
# print(pd.DataFrame.from_dict(Counter(c.get_issues(org, repo).priority.values), orient='index'))

# print(p.get_companies_for_contributors_based_on_devstats_data(org, repo))
# p.determine_bots_based_on_devstats_data()
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