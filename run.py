from datetime import datetime
import matplotlib.pyplot as plt
import src.analyzer as a
import src.crawler as c
import src.preprocesser as p
import src.visualizer as v

org = "kubernetes"
repo = "kubernetes"

###########################################################################
############################## CRAWLING ###################################
# c.crawl(org, repo)
# p.preprocess(org, repo)

###########################################################################
####################### ANALYSIS & VISUALIZATION ##########################

# metric #1: PR accepteance rate
print(a.calculate_pr_acceptance_rate_by_companies(org, repo))

# metric #2: processing time
a.calculate_issue_processing_time(org, repo)
issues = c.get_issues_with_processing_time(org, repo)
issues = v.filter_issues_after(issues, datetime(2017, 1, 1))
v.boxplot_issue_processing_time(issues)

# metric #3: response time
a.calculate_issue_reponse_time(org, repo)
issues = c.get_issues_with_response_time(org, repo)
v.boxplot_issue_reponse_time(issues)

# metric #4: prioritization
issue_kind_distribution = a.calculate_issue_kind_share_by_company(org, repo)
v.show_stacked_bar_chart_for_issue_kinds_by_company(issue_kind_distribution)

# other metrics
# print(a.calculate_avg_issue_response_time_by_company(org, repo))
# print(a.calculate_avg_issue_processing_time_by_company(org, repo))

# data inspection / controlling variables
issues = c.get_issues_with_company(org, repo)
v.show_stacked_bar_chart_for_issue_priorities_by_company(issues)


###########################################################################
############################# PLAYGROUND ##################################

issues = v.filter_issues_for_kind(issues, "bug")
issues = issues.loc[issues["priority"] < 3]
v.boxplot_issue_processing_time(issues)

# commentators = set(c.get_issues_with_comments(org, repo)["user_login"].values)
# for co in commentators:
#     if "bot" in co:
#         print(co)

plt.show()