import pandas as pd
import re

from cowidev.utils.clean import clean_count, clean_date
from cowidev.vax.manual.twitter.base import TwitterCollectorBase


class Zimbabwe(TwitterCollectorBase):
    def __init__(self, api):
        super().__init__(
            api=api,
            username="MoHCCZim",
            location="Zimbabwe",
            add_metrics_nan=True,
        )

    def _propose_df(self):
        regex = r"COVID-19 update: As at (\d{1,2} [a-zA-Z]+ 202\d), .* a total of ([\d ]+) people have been vaccinated"
        data = []
        for tweet in self.tweets:
            match = re.search(regex, tweet.full_text)
            if match:
                dt = clean_date(match.group(1), "%d %B %Y")
                total_vaccinations = clean_count(match.group(2))
                dt = clean_date(tweet.created_at)
                if self.stop_search(dt):
                    break
                data.append(
                    {
                        "date": dt,
                        "people_vaccinated": total_vaccinations,
                        "text": tweet.full_text,
                        "source_url": self.build_post_url(tweet.id),
                        "media_url": tweet.entities["media"][0]["media_url_https"]
                        if "media" in tweet.entities
                        else None,
                    }
                )
        return pd.DataFrame(data)


def main(api):
    Zimbabwe(api).to_csv()
