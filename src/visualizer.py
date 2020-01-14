from collections import defaultdict
from collections import Counter
from datetime import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns
import src.crawler as c
import src.preprocesser as p
import src.analyzer as a


def boxplot_issue_reponse_time(issues):
    plt.figure()
    _print_company_representation_in_data(issues)
    sns.boxplot(x="company", y="response_time", showfliers=False, showmeans=True, whis=[5, 75], data=issues, order=sorted(list(set(issues["company"].values))))

def boxplot_issue_processing_time(issues):
    plt.figure()
    _print_company_representation_in_data(issues)
    sns.boxplot(x="company", y="processing_time", showfliers=False, showmeans=True, whis=[5, 75], data=issues, order=sorted(list(set(issues["company"].values))))

def show_stacked_bar_chart_for_issue_priorities_by_company(df):
    df = df.loc[df["priority"] != 5]
    _print_company_representation_in_data(df)
    _normalized_stacked_bar_chart(df, "priority", "company")

def show_stacked_bar_chart_for_issue_kinds_by_company(df):
    print(df.sum())
    df = df.apply(lambda x: x / x.sum())
    df.transpose().plot.bar(stacked=True, figsize=(10,7))   

def filter_issues_for_kind(issues, kind):
    issues = issues.dropna(subset=["kind", "company"])
    issues[kind] = issues.apply(_add_dummy_var_for_kind, kind=kind, axis = 1)
    return issues.loc[issues[kind] == True]

def filter_issues_after(issues, time):
    issues = issues.dropna(subset=["created_at", "company"])
    issues["time"] = issues.apply(_add_dummy_var_for_time, time = time, axis = 1)
    return issues.loc[issues["time"] == True]

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

# reverse function of pd.melt()
def _boxplot_issue_processing_time_with_pd(org, repo):
    issues_df = c.get_issues_with_processing_time(org, repo)
    processing_time_by_company = defaultdict(list)

    for index, issue in issues_df.iterrows():
        processing_time_by_company[issue.company].append(issue.processing_time)

    # convert to pd.Series to cope with different list length when creating a df
    for company in processing_time_by_company:
        processing_time_by_company[company] = pd.Series(processing_time_by_company[company])
    
    processing_time_by_company_df = pd.DataFrame(processing_time_by_company)
    print(processing_time_by_company_df)
    print(list(processing_time_by_company.keys()))
    processing_time_by_company_df.boxplot(column=list(processing_time_by_company.keys()))
    
def _print_company_representation_in_data(df):
    print(pd.DataFrame.from_dict(Counter(df.company.values), orient='index'))

def _remove_outliers(df, column):
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    filter = (df[column] >= Q1 - 1.5 * IQR) & (df[column] <= Q3 + 1.5 * IQR)
    return df.loc[filter]   

def _stacked_bar_chart(df, attribute, group_by):
    df.groupby([group_by, attribute]).size().unstack().plot(kind='bar',stacked=True)
    
def _normalized_stacked_bar_chart(df, attribute, group_by):
    df.groupby([group_by, attribute]).size().groupby(level=0).apply(
        lambda x: 100 * x / x.sum()
    ).unstack().plot(kind='bar',stacked=True)

def _simple_stacked_bar_chart(df, group_by):
    y = df.set_index(group_by)
    z = y.groupby(group_by).mean()
    z.plot.bar(stacked=True)

######## PLAYGROUND ###########
# print(Counter(issues["company"].values))
# print(sum(i < 30.0 for i in list(issues["response_time"].values)))

# filter = (df_reponse["company"] == "RedHat")
# print(df_reponse.loc[filter].sort_values(by=["response_time"]))

# issues = issues.loc[issues["priority"] == 0]

# print(df["company"].isnull().values.ravel().sum())

# plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter())