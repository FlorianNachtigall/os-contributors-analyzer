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


def boxplot_issue_reponse_time(issues, companies=None):
    palette = {"Google":"C0","IBM":"C6","Microsoft":"C1", "Huawei":"C4", "RedHat":"C2", "VMware":"C3", "ZTE":"C7", "Fujitsu":"C5"}
    if not companies:
        companies = list(set(issues["company"].values))
    plt.figure()
    company_order = _get_company_order_by_median(issues, "response_time", companies)
    plot = sns.boxplot(x="company", y="response_time", showfliers=False, showmeans=True, whis=1.5, data=issues, order=company_order, palette=palette)
    plot.set_title("PR Response Time by Companies")
    _print_company_representation_in_data(issues)
    _add_median_label_to_plot(plot, issues, "response_time", company_order)

def boxplot_issue_processing_time(issues, companies=None):
    palette = {"Google":"C0","IBM":"C6","Microsoft":"C1", "Huawei":"C4", "RedHat":"C2", "VMware":"C3", "ZTE":"C7", "Fujitsu":"C5"}
    if not companies:
        companies = list(set(issues["company"].values))
    plt.figure()
    company_order = _get_company_order_by_median(issues, "processing_time", companies)
    plot = sns.boxplot(x="company", y="processing_time", showfliers=False, showmeans=True, whis=1.5, data=issues, order=company_order, palette=palette)
    plot.set_title("PR Processing Time by Companies")
    _print_company_representation_in_data(issues)
    _add_median_label_to_plot(plot, issues, "processing_time", company_order)

def _get_company_order_by_median(issues, metric_for_median, companies):
    medians = issues.groupby(['company'])[metric_for_median].median()
    medians = medians.loc[medians.index.isin(companies)]
    print(list(medians.sort_values().index))
    return list(medians.sort_values().index)

def _get_company_order_by_mean(issues, metric_for_median, companies):
    medians = issues.groupby(['company'])[metric_for_median].mean()
    medians = medians.loc[medians.index.isin(companies)]
    print(list(medians.sort_values().index))
    return list(medians.sort_values().index)

def _add_median_label_to_plot(plot, issues, metric_for_median, companies):
    medians = issues.groupby(['company'])[metric_for_median].median()
    medians = medians.loc[medians.index.isin(companies)].sort_values().round(decimals=-3).astype(int)
    vertical_offset = issues[metric_for_median].median() * 0.6 # offset from median for display
    for xtick in plot.get_xticks():
        plot.text(xtick, medians[xtick] + vertical_offset, medians[xtick], horizontalalignment='center', size='x-small', color='w', weight='semibold')

def _add_mean_label_to_plot(plot, issues, metric_for_median, companies):
    mean = issues.groupby(['company'])[metric_for_median].mean()
    mean = mean.loc[mean.index.isin(companies)].loc[_get_general_company_order()].round(decimals=2)
    # mean = mean.loc[mean.index.isin(companies)].sort_values().round(decimals=2)
    for xtick in plot.get_xticks():
        plot.text(xtick, 1.5, mean[xtick], horizontalalignment='center', size='x-small', color='w', weight='semibold')


def show_stacked_bar_chart_for_issue_priorities_by_company(df, companies):
    df = df.loc[df['company'].isin(companies)]
    print("Priority Overall Distribution: " + str(Counter(df.priority.values)))
    df = df.loc[df["priority"] != 5]
    print("Priority Mean (without 5 / non-prioritized issues): " + str(df.priority.mean()))
    print("Priority Median (without 5 / non-prioritized issues): " + str(df.priority.median()))
    _print_company_representation_in_data(df)
    # order = _get_company_order_by_mean(df, "priority", companies)
    order = _get_general_company_order()
    plot = _normalized_stacked_chart(df, "priority", "company", "bar", order=order)
    _add_mean_label_to_plot(plot, df, "priority", companies)
    plt.legend(bbox_to_anchor=(1.04,0.5), loc="center left", title="Priority", borderaxespad=0)
    plt.tight_layout(rect=[0,0,0.95,1])

def _get_general_company_order():
    return ['Google', 'Microsoft', 'RedHat', 'VMware', 'Huawei', 'Fujitsu', 'IBM', 'ZTE']

def show_bar_chart_for_pr_rejection_rates_by_company(acceptance_rates_dict, overall_pr_acceptance_rate):
    plt.figure()    
    df = pd.DataFrame.from_dict(acceptance_rates_dict, orient="index", columns=["acceptance_rate"])
    df["rejection_rate"] = 1 - df["acceptance_rate"]
    company_order = list(df.sort_values(by=["rejection_rate"]).index)
    plot = sns.barplot(x=df.index, y="rejection_rate", data=df, order=company_order)
    plot.set_title("PR Rejection Rate by Companies")
    plot.set_xlabel("company")
    plot.set(ylim=(0, 1))
    plot.axhline(1 - overall_pr_acceptance_rate, ls='--')
    xlocs, _ = plt.xticks()
    for i, v in enumerate(df["rejection_rate"].sort_values()):
        plt.text(xlocs[i] - 0.25, v + 0.01, "{0:.2f}".format(v))

def show_area_chart_for_pr_rejection_rates_over_time(pulls):
    users = c.get_issue_authors_with_company("kubernetes", "kubernetes")
    pulls = p.merge_issues_with_company_column(pulls, users)
    pulls["company"].fillna("others", inplace=True)

    pulls = pulls.dropna(subset=["created_at", "closed_at"])
    pulls = p.add_dummy_column_for_month(pulls)
    pulls = p.add_dummy_column_for_pr_merge_state(pulls)
    companies = set(pulls["company"].values)
    _, axs = plt.subplots(nrows=len(companies))
    for i, company in enumerate(companies):
        print(company)
        company_pulls = pulls.loc[pulls["company"] == company]
        _normalized_stacked_chart(company_pulls, "pr_is_merged", "month", "area", axs[i])

def show_line_chart_for_metrics_over_time(pulls, issues_with_processing_time, issues_with_response_time, companies):
    pulls = pulls.dropna(subset=["created_at", "closed_at"])
    pulls = p.determine_company_for_issues_with_history(pulls)
    pulls['company'] = np.where(pulls['company'].isin(companies), pulls['company'], 'unknown')
    pulls["company"].fillna("others", inplace=True)

    # pulls = p.add_dummy_column_for_rounded_year(pulls)
    pulls = p.add_dummy_column_for_pr_merge_state(pulls)
    pulls = p.add_dummy_column_for_month(pulls)
    issues_with_processing_time = p.add_dummy_column_for_month(issues_with_processing_time)
    issues_with_response_time = p.add_dummy_column_for_month(issues_with_response_time)
    
    df = pulls.groupby(["month"])["pr_is_merged"].mean().reset_index()
    print(issues_with_processing_time.groupby(["month"])["processing_time"].median().reset_index())
    df["processing_time"] = issues_with_processing_time.groupby(["month"])["processing_time"].median().reset_index()["processing_time"]
    df["response_time"] = issues_with_response_time.groupby(["month"])["response_time"].median().reset_index()["response_time"]
    df = df.set_index(["month"])
    _, ax = plt.subplots()
    ax1 = df[["pr_is_merged"]].plot(kind="line", ax=ax, colormap='Spectral')
    ax1.legend(loc=(0.1,0.9))
    ax2 = ax1.twinx()
    ax2.spines['right'].set_position(('axes', 1.0))
    ax1.set_title("PR Acceptance Rate, Processing and Response Time over the Community Lifetime", fontsize=10)
    ax1.set_ylabel("acceptance rate")
    ax2.set_ylabel("time [s]")
    df[["processing_time", "response_time"]].plot(kind="line", logy=True, mark_right=True, ax=ax2)
    # df2.plot(ax=ax2)

def show_line_chart_for_pr_rejection_rates_over_time(pulls, based_on_devstats_data=False, companies=[]):
    pulls = pulls.dropna(subset=["created_at", "closed_at"])
    
    if based_on_devstats_data:
        pulls = p.determine_company_for_issues_with_history(pulls)
        pulls['company'] = np.where(pulls['company'].isin(companies), pulls['company'], 'unknown')
    else:
        users = c.get_issue_authors_with_company("kubernetes", "kubernetes")
        pulls = p.merge_issues_with_company_column(pulls, users)

    pulls["company"].fillna("others", inplace=True)
    pulls = p.add_dummy_column_for_rounded_year(pulls)
    # pulls = p.add_dummy_column_for_month(pulls)
    pulls = p.add_dummy_column_for_pr_merge_state(pulls)
    # companies = set(pulls["company"].values)
    # _normalized_stacked_chart(pulls, "company", "month", "line")
    df = pulls.groupby(["company", "year"])["pr_is_merged"].mean().unstack(level=0)
    print(df)
    plt = df.plot(kind="line")
    plt.set_ylabel("acceptance rate")
    plt.set_title("PR Acceptance Rate over the Community Lifetime", fontsize=10)


def show_stacked_bar_chart_for_issue_kinds_by_company(df):
    print(df.sum())
    df = df.apply(lambda x: x / x.sum())
    df.transpose().loc[_get_general_company_order()].plot.bar(stacked=True, figsize=(10,7))
    # plt.legend(bbox_to_anchor=(0,1.02,1,0.2), loc="lower left", title="Priority", mode="expand", borderaxespad=0, ncol=5)
    plt.legend(bbox_to_anchor=(1.04,0.5), loc="center left", title="PR Kind", borderaxespad=0)
    plt.tight_layout(rect=[0,0,0.95,1])

def show_stacked_area_chart_for_issue_contributor_affiliation_over_time(issues, companies):
    issues = issues.dropna(subset=["created_at"])
    # issues["company"].fillna("others", inplace=True)
    issues['company'] = np.where(issues['company'].isin(companies), issues['company'], 'others')
    issues = p.add_dummy_column_for_rounded_year(issues)
    order = _get_general_company_order() + ['others']
    print(order)
    plot = _normalized_stacked_chart(issues, "company", "year", "area", order=order)
    plot.set_title("PR Composition by Companies over the Community Lifetime", fontsize=10)
    plot.set_ylabel("share of PRs [%]")
    
    issues = p.add_dummy_column_for_month(issues)
    # _normalized_stacked_chart(issues, "company", "month", "area")
    plot =_stacked_chart(issues, "company", "month", "area", order=order)
    plot.set_title("Total PRs over the Community Lifetime", fontsize=10)
    plot.set_ylabel("PRs [#/month]")
    plt.legend(bbox_to_anchor=(1.04,0.5), loc="center left", title="company", borderaxespad=0)
    plt.tight_layout(rect=[0,0,0.95,1])


def show_stacked_area_chart_for_company_issues_over_time(issues, companies):
    issues = issues.dropna(subset=["company", "created_at"])
    issues = issues.loc[issues['company'].isin(companies)]
    issues = p.add_dummy_column_for_rounded_year(issues)
    _normalized_stacked_chart(issues, "company", "year", "area", order=_get_general_company_order())
    # issues = p.add_dummy_column_for_month(issues)
    # _normalized_stacked_chart(issues, "company", "month", "area")


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

def _stacked_chart(df, attribute, group_by, chart_type, order=None):
    df = df.groupby([group_by, attribute]).size().unstack()
    if order:
        df = df.reindex(order, axis=1)
        #df = df.loc(order)
    return df.plot(kind=chart_type, stacked=True)
    
def _normalized_stacked_chart(df, attribute, group_by, chart_type, subplot=None, order=None):
    df = df.groupby([group_by, attribute]).size().groupby(level=0).apply(
        lambda x: 100 * x / x.sum()
    ).unstack()
    if order:
        print(df)
        df = df.reindex(order, axis=1)
        #df = df.loc(order)
    return df.plot(kind=chart_type, stacked=True, ax=subplot)

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