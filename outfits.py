from time import sleep
from typing import Callable, Dict, List, Set, Tuple, Union

from ps2_census import Collection, Join, Query
from ps2_census.enums import World

outfit_query_factory: Callable[[], Query] = (Query(Collection.OUTFIT).get_factory())

world_events_query_factory: Callable[[], Query] = Query(Collection.WORLD_EVENT).join(
    Join(Collection.OUTFIT).on("outfit_id").to("outfit_id").outer(0).inject_at("outfit")
).get_factory()


def get_outfit(service_id: str, outfit_name: str) -> Dict[str, Union[int, str]]:
    print(f"Getting outfit")

    res: dict = outfit_query_factory().set_service_id(service_id).filter(
        "name", outfit_name
    ).get()

    if "returned" not in res:
        print(res)
        raise Exception

    assert res["returned"] == 1

    raw = res["outfit_list"][0]

    print(f"Got outfit")

    return {
        "id": int(raw["outfit_id"]),
        "name": raw["name"],
        "alias": raw["alias"],
        "member_count": int(raw["member_count"]),
    }


def get_outfit_world_events(
    service_id: str,
    worlds: List[World],
    from_ts: int,
    to_ts: int,
    max_query_events: int = 100,
) -> List[dict]:
    print(f"Getting outfit world events on {worlds} between {from_ts} and {to_ts}")

    events: List[dict] = []

    min_ts: int = to_ts

    while min_ts > from_ts:
        query: Query = (
            world_events_query_factory()
            .set_service_id(service_id=service_id)
            .filter("world_id", ",".join((str(w.value) for w in worlds)))
            .filter("before", min_ts)
            .limit(max_query_events)
        )
        res: dict = query.get()

        if "returned" not in res:
            print(res)
            raise Exception

        iteration_events: List[dict] = res["world_event_list"]

        print(f"Got {len(iteration_events)} events prior to {min_ts}")

        if iteration_events:
            min_ts = min((int(e["timestamp"]) for e in iteration_events))

            for e in filter(lambda x: int(x["timestamp"]) >= from_ts, iteration_events):
                events.append(e)

            sleep(0.5)
        else:
            break

    assert min((int(e["timestamp"]) for e in events)) >= from_ts
    assert max((int(e["timestamp"]) for e in events)) <= to_ts

    print(f"Got {len(events)} outfit events")

    return events


def generate_outfit_data(service_id: str, from_ts: int, to_ts: int):
    outfit_events = get_outfit_world_events(
        service_id=service_id, worlds=(World.SOLTECH,), from_ts=from_ts, to_ts=to_ts,
    )

    outfits: Set[Tuple[tuple]] = set(
        (tuple(e["outfit"].items()) for e in outfit_events)
    )
    print(outfits)
