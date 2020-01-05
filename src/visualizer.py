import pandas as pd
import matplotlib
import numpy as np
import crawler as c
from collections import defaultdict

def boxplot_issue_processing_time(org, repo):
    issues_df = c.get_issues_with_processing_time(org, repo)
    # processing_time_by_company = dict.fromkeys(set(issues_df["company"].values), [])
    processing_time_by_company = defaultdict(list)

    for index, issue in issues_df.iterrows():
        processing_time_by_company[issue.company].append(issue.processing_time)

    # convert to pd.Series to cope with different list length when creating a df
    for company in processing_time_by_company:
        processing_time_by_company[company] = pd.Series(processing_time_by_company[company])
    
    processing_time_by_company_df = pd.DataFrame(processing_time_by_company)
    print(processing_time_by_company_df)
    print(list(processing_time_by_company.keys()))
    boxplot = processing_time_by_company_df.boxplot(column=list(processing_time_by_company.keys()))
    print(boxplot)

boxplot_issue_processing_time("kubernetes", "kubernetes")