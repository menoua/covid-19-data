import re

import pandas as pd

from cowidev.utils.clean import clean_count, clean_date
from cowidev.vax.manual.twitter.base import TwitterCollectorBase


class Paraguay(TwitterCollectorBase):
    def __init__(self, api, **kwargs):
        super().__init__(api=api, username="msaludpy", location="Paraguay", add_metrics_nan=True, **kwargs)

    def _propose_df(self):
        regex = r"VACUNACIÓN #COVID19 \| Reporte del (\d{1,2}\.\d{1,2}\.202\d) - \d{1,2}:\d{1,2}"
        data = []
        for tweet in self.tweets:
            match = re.search(regex, tweet.full_text)
            if match:
                regex_doses = r"Total Dosis Administradas: ([\d\.]+)"
                total_vaccinations = re.search(regex_doses, tweet.full_text)
                if total_vaccinations:
                    total_vaccinations = clean_count(total_vaccinations.group(1))
                else:
                    total_vaccinations = pd.NA
                regex_people = r"Total personas vacunadas: ([\d\.]+)"
                people_vaccinated = re.search(regex_people, tweet.full_text)
                if people_vaccinated:
                    people_vaccinated = clean_count(people_vaccinated.group(1))
                else:
                    people_vaccinated = pd.NA
                people_fully_vaccinated = total_vaccinations - people_vaccinated
                dt = clean_date(match.group(1), "%d.%m.%Y")
                if self.stop_search(dt):
                    break
                data.append(
                    {
                        "date": dt,
                        "total_vaccinations": total_vaccinations,
                        "people_vaccinated": people_vaccinated,
                        "people_fully_vaccinated": people_fully_vaccinated,
                        "text": tweet.full_text,
                        "source_url": 1,  # pan.build_post_url(tweet.id),
                        "media_url": tweet.extended_entities["media"][0]["media_url_https"],
                    }
                )
        df = pd.DataFrame(data)
        return df


def main(api):
    Paraguay(api).to_csv()
