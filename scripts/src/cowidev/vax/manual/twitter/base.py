import os
import pandas as pd
from cowidev.utils import paths


COLUMN_METRICS_ALL = [
    "total_vaccinations",
    "people_vaccinated",
    "people_fully_vaccinated",
]


class TwitterCollectorBase:
    def __init__(
        self,
        api,
        username: str,
        location: str,
        add_metrics_nan: bool = False,
        output_path=None,
        num_tweets=100,
    ):
        self.username = username
        self.location = location
        self.tweets = api.get_tweets(self.username, num_tweets)
        self.add_metrics_nan = add_metrics_nan

        self.tweets_relevant = []
        self.output_path = self._set_output_path(output_path)
        self._data_old = self._get_current_data()

    def _set_output_path(self, output_path):
        if output_path is None:
            return paths.out_vax(self.location, proposal=True)
        else:
            raise AttributeError("Either specify attribute `paths` or method argument `output_path`")

    def _get_current_data(self):
        if os.path.isfile(self.output_path):
            return pd.read_csv(self.output_path)
        else:
            None

    @property
    def last_update(self):
        if self._data_old is not None:
            return self._data_old.date.max()
        else:
            return None

    def _propose_df(self):
        raise NotImplementedError

    def propose_df(self):
        df = (
            self._propose_df()
            .pipe(self._add_metrics)
            .pipe(self.merge_with_current_data)
            .pipe(self._order_columns)
            .reset_index(drop=True)
            .sort_values("date")
        )
        return df

    def _add_metrics(self, df):
        if isinstance(self.add_metrics_nan, list):
            for col in self.add_metrics_nan:
                df = df.assign(**{col: pd.NA})
        elif self.add_metrics_nan:
            for col in COLUMN_METRICS_ALL:
                if col not in df.columns:
                    df = df.assign(**{col: pd.NA})
        return df

    def _order_columns(self, df):
        column_metrics = []
        column_optional = []
        for col in COLUMN_METRICS_ALL:
            if col in df.columns:
                column_metrics.append(col)
        if "media_url" in df:
            column_optional.append("media_url")

        df = df[["date"] + column_metrics + ["source_url"] + column_optional + ["text"]]
        return df

    def build_post_url(self, tweet_id: str):
        return f"https://twitter.com/{self.username}/status/{tweet_id}"

    def merge_with_current_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return self._data_old
        if self._data_old is not None:
            df_current = self._data_old[~self._data_old.date.isin(df.date)]
            df = pd.concat([df, df_current]).sort_values(by="date")
        return df

    def stop_search(self, dt):
        if self._data_old is None:
            return False
        elif dt >= self.last_update:
            return False
        elif dt < self.last_update:
            return True

    def to_csv(self):
        df = self.propose_df()
        df.to_csv(self.output_path, index=False)
