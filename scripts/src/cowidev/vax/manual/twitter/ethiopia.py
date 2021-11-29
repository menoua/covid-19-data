import pandas as pd
import re

from cowidev.vax.manual.twitter.base import TwitterCollectorBase
from cowidev.utils.clean import clean_date


class Ethiopia(TwitterCollectorBase):
    def __init__(self, api):
        super().__init__(
            api=api,
            username="FMoHealth",
            location="Ethiopia",
            add_metrics_nan=["total_vaccinations"],
        )

    def _propose_df(self):
        regex = r"ባለፉት 24 .*"
        data = []
        for tweet in self.tweets:
            if re.search(regex, tweet.full_text):
                dt = clean_date(tweet.created_at)
                if self.stop_search(dt):
                    break
                data.append(
                    {
                        "date": dt,
                        "text": tweet.full_text,
                        "source_url": self.build_post_url(tweet.id),
                        "media_url": tweet.extended_entities["media"][1]["media_url_https"],
                    }
                )
        return pd.DataFrame(data)


def main(api):
    Ethiopia(api).to_csv()
