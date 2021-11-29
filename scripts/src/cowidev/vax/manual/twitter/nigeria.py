import re

import pandas as pd

from cowidev.utils.clean import clean_count, clean_date
from cowidev.vax.manual.twitter.base import TwitterCollectorBase


class Nigeria(TwitterCollectorBase):
    def __init__(self, api, **kwargs):
        super().__init__(api=api, username="NphcdaNG", location="Nigeria", add_metrics_nan=True, **kwargs)

    def _propose_df(self):
        regex_1 = (
            r"COVID-19 Vaccination Update:\n\n1st and second dose — (([a-zA-Z]+) (\d{1,2})(?:th|nd|rd|st) (202\d)), in"
            r" 36 States \+ the FCT\. \n\n([0-9,]+) eligible "
            r"Nigerians have been vaccinated with first dose while ([0-9,]+) of Nigerians vaccinated with 1st dose"
            r" have collected their 2nd dose\."
        )
        regex_2 = (
            r"COVID-19 Vaccination Update for (([a-zA-Z]+) (\d{1,2})(?:th|nd|rd|st),? (202\d)), in 36 States \+ the"
            r" FCT\. "
        )
        regex_3 = r"COVID-19 Vaccination Update"
        data = []
        for tweet in self.tweets:
            match_1 = re.search(regex_1, tweet.full_text)
            match_2 = re.search(regex_2, tweet.full_text)
            match_3 = re.search(regex_3, tweet.full_text)
            if match_1:
                people_vaccinated = clean_count(match_1.group(5))
                people_fully_vaccinated = clean_count(match_1.group(6))
                dt = clean_date(" ".join(match_1.group(2, 3, 4)), "%B %d %Y")
                if self.stop_search(dt):
                    break
                data.append(
                    {
                        "date": dt,
                        "total_vaccinations": people_vaccinated + people_fully_vaccinated,
                        "people_vaccinated": people_vaccinated,
                        "people_fully_vaccinated": people_fully_vaccinated,
                        "text": tweet.full_text,
                        "source_url": self.build_post_url(tweet.id),
                        "media_url": tweet.extended_entities["media"][0]["media_url_https"],
                    }
                )
            elif match_2:
                dt = clean_date(" ".join(match_2.group(2, 3, 4)), "%B %d %Y")
                if self.stop_search(dt):
                    break
                data.append(
                    {
                        "date": dt,
                        "text": tweet.full_text,
                        "source_url": self.build_post_url(tweet.id),
                        "media_url": tweet.extended_entities["media"][0]["media_url_https"],
                    }
                )
            elif match_3:
                data.append(
                    {
                        "text": tweet.full_text,
                        "source_url": self.build_post_url(tweet.id),
                        "media_url": tweet.extended_entities["media"][0]["media_url_https"],
                    }
                )
        df = pd.DataFrame(data)
        return df


def main(api):
    Nigeria(api).to_csv()
