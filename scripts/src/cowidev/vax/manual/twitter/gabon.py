import pandas as pd
import re

from cowidev.vax.manual.twitter.base import TwitterCollectorBase
from cowidev.utils.clean import clean_date


class Gabon(TwitterCollectorBase):
    def __init__(self, api):
        super().__init__(
            api=api,
            username="SanteGOUVGA",
            location="Gabon",
            add_metrics_nan=True,
        )

    def _propose_df(self):
        regex = r"Recevez la situation .* au (\d{1,2} [a-z]+ 202\d)\."
        data = []
        for tweet in self.tweets:
            match = re.search(regex, tweet.full_text)
            if match:
                dt = clean_date(match.group(1), "%d %B %Y", lang="fr")
                if self.stop_search(dt):
                    break
                data.append(
                    {
                        "date": dt,
                        "text": tweet.full_text,
                        "source_url": self.build_post_url(tweet.id),
                        "media_url": tweet.entities["media"][0]["media_url_https"]
                        if "media" in tweet.entities
                        else None,
                    }
                )
        return pd.DataFrame(data)


def main(api):
    Gabon(api).to_csv()
