import re

import pandas as pd

from cowidev.vax.manual.twitter.base import TwitterCollectorBase
from cowidev.utils.clean.dates import from_tz_to_tz, clean_date


class Panama(TwitterCollectorBase):
    def __init__(self, api):
        super().__init__(
            api=api,
            username="MINSAPma",
            location="Panama",
            add_metrics_nan=True,
        )

    def _propose_df(self):
        regex = r"Comunicado N° (\d{3,4}).*"
        data = []
        for tweet in self.tweets:
            match = re.search(regex, tweet.full_text)
            if match:
                dt = clean_date(from_tz_to_tz(tweet.created_at, to_tz="America/Panama"))
                if self.stop_search(dt):
                    break
                data.append(
                    {
                        "date": dt,
                        "text": tweet.full_text,
                        "source_url": self.build_post_url(tweet.id),
                        "num": match.group(1),
                    }
                )
                self.tweets_relevant.append(tweet)
        df = pd.DataFrame(data)
        df = df.drop_duplicates(subset=["num"], keep="last")
        return df


def main(api):
    Panama(api).to_csv()
