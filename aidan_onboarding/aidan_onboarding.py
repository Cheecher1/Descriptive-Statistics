# !pip install snowflake
# !pip install "snowflake-connector-python[pandas]"

import matplotlib.pyplot as plt
import pandas as pd
import snowflake
import snowflake.connector

# Connect to snowflake
conn = snowflake.connector.connect(
    user="<username>",
    password="<password>",
    account="<account>",
    warehouse="<warehouse>",
    database="<database>",
    schema="<schema>",
)

# Grab 1m rows from table in snowflake
cur = conn.cursor()
cur.execute(
    """
SELECT *
FROM "CLIENT"."schema"."table"
LIMIT 1000000;
"""
)
# Put rows into dataframe
test_df = cur.fetch_pandas_all()
cur.close()

# Pickle 1m row dataframe for local storage
test_df.to_pickle("pickled_data")

# Create new dataframe from the pickled data
data_df = pd.read_pickle("pickled_data")


# Class for descriptive statistics
class stats:
    def __init__(self, dataframe):
        self.frame = dataframe
        self.variables = self.frame.columns

    def get_frame(self):
        return self.frame

    def get_variables(self):
        return self.variables

    def get_mean(self, var, feat=None, title="mean", hist=False, maxsize=10):
        if feat is None:
            return self.frame[var].mean(axis="index")
        else:
            retframe = self.frame.groupby([feat])[var].mean()
            if hist is True:
                rethist = self._make_bar(retframe, title, var, feat, maxsize, "Mean")
            return retframe.to_frame(title)

    def get_median(self, var, feat=None, title="median", hist=False, maxsize=10):
        if feat is None:
            return self.frame[var].median(axis="index")
        else:
            retframe = self.frame.groupby([feat])[var].median()
            if hist is True:
                rethist = self._make_bar(retframe, title, var, feat, maxsize, "Median")
            return retframe.to_frame(title)

    def get_std(self, var, feat=None, title="std", hist=False, maxsize=10):
        if feat is None:
            return self.frame[var].std(axis="index")
        else:
            retframe = self.frame.groupby([feat])[var].std()
            if hist is True:
                rethist = self._make_bar(retframe, title, var, feat, maxsize, "Std Dev")
            return retframe.to_frame(title)

    def get_skew(self, var, feat=None, title="skew", hist=False, maxsize=10):
        if feat is None:
            return self.frame[var].skew(axis="index")
        else:
            retframe = self.frame.groupby([feat])[var].skew()
            if hist is True:
                rethist = self._make_bar(retframe, title, var, feat, maxsize, "Skew")
            return retframe.to_frame(title)

    def get_kurtosis(self, var, feat=None, title="kurt", hist=False, maxsize=10):
        if feat is None:
            return self.frame[var].kurtosis(axis="index")
        else:
            retframe = self.frame.groupby([feat])[var].apply(pd.DataFrame.kurt)
            if hist is True:
                rethist = self._make_bar(
                    retframe, title, var, feat, maxsize, "Kurtosis"
                )
            return retframe.to_frame(title)

    def get_all(self, var, feat=None, hist=False, maxsize=10):
        mean = self.get_mean(var, feat, "mean")
        median = self.get_median(var, feat, "median")
        std = self.get_std(var, feat, "std")
        skew = self.get_skew(var, feat, "skew")
        kurtosis = self.get_kurtosis(var, feat, "kurt")
        if feat is not None:
            return mean.join([median, std, skew, kurtosis])
        else:
            if hist is True:
                histframe = self.frame[var]
                rethist = self._make_bar(histframe, "all", var, feat, maxsize, "var")
            return [mean, median, std, skew, kurtosis]

    def make_html(self, frame, path=None):
        html = frame.to_html()
        f = open("test.html", "w")
        buf = [html]
        if path is not None:
            img = '<img src = "' + path + '" alt ="cfg">\n'
            buf.append(img)
        f.write(buf)
        f.close()

    def _make_hist(self, frame, title, xlabel, ylabel, bins=100):
        hist = frame.hist(bins=bins)
        hist.set_title(title)
        hist.set_xlabel(xlabel)
        ylabel = ylabel + " freq"
        hist.set_ylabel(ylabel)
        fig = hist.get_figure()
        fig.savefig(title + "_hist.png")
        return hist

    def _compress(self, frame, size, marker):
        series = frame.squeeze()
        top = series.nlargest(size)
        labels = top.keys()
        remaining = series.drop(labels)
        remainstr = str(remaining.size) + " others"
        rem_mean = remaining.mean("index")
        rem_series = pd.Series({remainstr: rem_mean})
        combined = pd.concat([top, rem_series])
        return combined.to_frame(marker)

    def _make_bar(self, frame, title, xlabel, ylabel, maxsize=10, marker="default"):
        if frame.size > maxsize:
            frame = self._compress(frame, maxsize, marker)
        bar = frame.plot.bar()
        bar.set_title(title)
        bar.set_xlabel(xlabel)
        bar.set_ylabel(ylabel)
        fig = bar.get_figure()
        fig.savefig(title + "_bar.png")
        return bar
