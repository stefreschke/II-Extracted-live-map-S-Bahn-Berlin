"""
Integration step for S Bahn Berlin data. Run this file to load and convert S Bahn data in nearby
files accordingly.
"""
import json
import logging
import pandas as pd
from pandas.io.json import json_normalize
import converting
from log_stuff import init_logger

logger = logging.getLogger('extraction')


def main():
    """
    Execution of integration step.
    :return:
    """
    with open('../data/traffic_data.json') as file:
        counter = 1
        my_fancy_df = None
        for index, line in enumerate(file):
            parts = line.split("|")
            timestamp = parts[0]
            suspected_json = "|".join(parts[1:])
            try:
                data_set = json.loads(suspected_json)
            except json.decoder.JSONDecodeError:
                logger.warning("Invalid line at {}, input was {}".format(index, line))
                continue
            entries = data_set["t"]
            df = converting.convert_sub_data_frame(json_normalize(entries))
            df['messpunkt'] = df.apply(lambda x: index, axis=1)
            my_fancy_df = pd.concat([my_fancy_df, df] if my_fancy_df is not None else [df],
                                    sort=False)
            logger.debug("Line %d processed, df has %d entries", index, my_fancy_df.size)
            if my_fancy_df.size > 1000000:
                filename = "../data/frame_{:03d}.pkl".format(counter)
                my_fancy_df.to_pickle(filename)
                logger.info("Wrote df to {}".format(filename))
                my_fancy_df = None
                counter += 1



if __name__ == '__main__':
    init_logger()
    main()
