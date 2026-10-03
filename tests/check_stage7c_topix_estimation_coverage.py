from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]

EVENTS = (
    ROOT
    / "data/interim/paper2/market_reaction/stage7_event_dates.csv"
)

TOPIX = (
    ROOT
    / "data/interim/paper2/market_reaction/topix_returns.csv"
)

EST_START = -120
EST_END = -20
MIN_OBS = 60


def main():
    events = pd.read_csv(EVENTS, low_memory=False)
    topix = pd.read_csv(TOPIX, low_memory=False)

    events["eventTradingDate"] = pd.to_datetime(
        events["eventTradingDate"],
        errors="raise",
    ).dt.normalize()

    topix["tradingDate"] = pd.to_datetime(
        topix["tradingDate"],
        errors="raise",
    ).dt.normalize()

    topix = (
        topix
        .sort_values("tradingDate")
        .drop_duplicates("tradingDate")
        .reset_index(drop=True)
    )

    trading_dates = topix["tradingDate"].tolist()
    date_to_pos = {
        d: i
        for i, d in enumerate(trading_dates)
    }

    rows = []

    for row in events.itertuples(index=False):

        event_date = row.eventTradingDate

        pos = date_to_pos.get(event_date)

        if pos is None:
            rows.append({
                "edinetCode": getattr(row, "edinetCode", None),
                "curr_docID": getattr(row, "curr_docID", None),
                "secCode": getattr(row, "secCode", None),
                "eventTradingDate": event_date,
                "topixEventDatePresent": False,
                "topixEstimationObs": 0,
                "meets60ObsMinimum": False,
            })
            continue

        start_pos = pos + EST_START
        end_pos = pos + EST_END

        # Inclusive [-120, -20]
        valid_start = max(start_pos, 0)
        valid_end = min(end_pos, len(trading_dates) - 1)

        if valid_end < valid_start:
            n_obs = 0
        else:
            n_obs = valid_end - valid_start + 1

        rows.append({
            "edinetCode": getattr(row, "edinetCode", None),
            "curr_docID": getattr(row, "curr_docID", None),
            "secCode": getattr(row, "secCode", None),
            "eventTradingDate": event_date,
            "topixEventDatePresent": True,
            "topixEstimationObs": n_obs,
            "meets60ObsMinimum": n_obs >= MIN_OBS,
        })

    out = pd.DataFrame(rows)

    print("\n=== Stage 7C TOPIX estimation-window coverage ===\n")

    print(f"Events:                         {len(out):,}")
    print(
        f"TOPIX event dates present:      "
        f"{out['topixEventDatePresent'].sum():,}"
    )
    print(
        f"Meet >= {MIN_OBS} obs minimum:       "
        f"{out['meets60ObsMinimum'].sum():,}"
    )
    print(
        f"Fail >= {MIN_OBS} obs minimum:       "
        f"{(~out['meets60ObsMinimum']).sum():,}"
    )

    print("\nEstimation observation counts:\n")
    print(
        out["topixEstimationObs"]
        .describe(
            percentiles=[0.01, 0.05, 0.10, 0.25, 0.50]
        )
        .to_string()
    )

    failures = out[
        ~out["meets60ObsMinimum"]
    ].copy()

    if len(failures):
        print("\n=== Events failing TOPIX history requirement ===\n")

        print(
            failures[
                [
                    "edinetCode",
                    "curr_docID",
                    "secCode",
                    "eventTradingDate",
                    "topixEstimationObs",
                ]
            ]
            .sort_values(
                [
                    "eventTradingDate",
                    "edinetCode",
                ]
            )
            .to_string(index=False)
        )

        print("\nFailure event-date range:")
        print(
            failures["eventTradingDate"].min(),
            "through",
            failures["eventTradingDate"].max(),
        )
    else:
        print(
            "\nPASS: every Stage 7B event has at least "
            f"{MIN_OBS} TOPIX observations in "
            f"[{EST_START}, {EST_END}]."
        )


if __name__ == "__main__":
    main()