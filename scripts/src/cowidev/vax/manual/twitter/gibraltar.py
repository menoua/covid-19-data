import requests

from PIL import Image
import numpy as np
import pandas as pd

from cowidev.vax.manual.twitter.base import TwitterCollectorBase
from cowidev.utils.clean import clean_date


class Gibraltar(TwitterCollectorBase):
    def __init__(self, api, **kwargs):
        super().__init__(api=api, username="GibraltarGov", location="Gibraltar", add_metrics_nan=True, **kwargs)

    def _propose_df(self):
        max_iter = 50
        dist_th = 8.7
        col_dominant = [1, 97, 207]
        records = []
        for tweet in self.tweets[:max_iter]:
            cond = "media" in tweet.entities and len(tweet.full_text) < 30
            if cond:
                url = tweet.extended_entities["media"][0]["media_url_https"]
                # print(url)
                im = Image.open(requests.get(url, stream=True).raw, formats=["png", "jpeg"])
                pixel_values = [x for i, x in enumerate(im.getdata()) if i < 100000]
                h = pd.value_counts(pixel_values, normalize=True).index[0][:3]
                # print(h)
                dist = np.linalg.norm(np.array(h) - np.array(col_dominant))
                if dist < dist_th:
                    # print("Found:", hist[0])
                    # print(tweet.full_text)
                    dt = clean_date(tweet.created_at)
                    if self.stop_search(dt):
                        break
                    records.append(
                        {
                            "date": dt,
                            "text": tweet.full_text,
                            "source_url": self.build_post_url(tweet.id),
                            "media_url": url,
                        }
                    )
        df = pd.DataFrame(records)
        return df


def main(api):
    Gibraltar(api).to_csv()
