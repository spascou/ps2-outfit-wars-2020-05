import csv
import json
import time
from collections import Counter
from typing import Callable, Dict, Iterable, List, Tuple, Union

from ps2_census import Collection, Join, Query
from ps2_census.enums import Faction
from slugify import slugify

from utils import batch

ACTIVITY_PERIOD: int = 12 * 60 * 60

character_events_query_factory: Callable[[], Query] = Query(
    Collection.CHARACTERS_EVENT
).join(
    Join(Collection.ACHIEVEMENT)
    .on("achievement_id")
    .to("achievement_id")
    .inject_at("achievement")
).join(
    Join(Collection.CHARACTER)
    .outer(0)
    .on("character_id")
    .to("character_id")
    .inject_at("character")
).join(
    Join(Collection.CHARACTER)
    .on("attacker_character_id")
    .to("character_id")
    .inject_at("attacker_character")
).join(
    Join(Collection.ITEM)
    .on("attacker_weapon_id")
    .to("item_id")
    .inject_at("attacker_weapon_item")
).join(
    Join(Collection.VEHICLE)
    .on("attacker_vehicle_id")
    .to("vehicle_id")
    .inject_at("vehicle")
).sort(
    ("timestamp", -1)
).get_factory()

outfit_members_query_factory: Callable[[], Query] = (
    Query(Collection.OUTFIT)
    .join(
        Join(Collection.OUTFIT_MEMBER)
        .on("outfit_id")
        .to("outfit_id")
        .outer(0)
        .list(1)
        .inject_at("members")
        .nest(
            Join(Collection.CHARACTER)
            .on("character_id")
            .to("character_id")
            .outer(0)
            .inject_at("character")
        )
    )
    .get_factory()
)


def get_character_events(
    service_id: str,
    character_ids: List[int],
    from_ts: int,
    to_ts: int,
    types: List[str] = (
        "ACHIEVEMENT",
        "DEATH",
        "KILL",
        "VEHICLE_DESTROY",
        "FACILITY_CHARACTER",
    ),
    max_query_events: int = 250,
    max_query_character_ids: int = 10,
    time_step: int = 60 * 10,
    custom_filter: Callable[[dict], bool] = lambda _: True,
):
    print(
        f"Getting character {types} events for {len(character_ids)} characters between {from_ts} and {to_ts}"
    )

    queries_count: int = 0

    events: List[dict] = []

    batch_character_ids: List[dict]
    for batch_character_ids in batch(character_ids, max_query_character_ids):
        print(f"Getting events for characters {batch_character_ids}")

        current_time: int = from_ts
        while current_time < to_ts:
            lower_bound: int = current_time
            upper_bound: int = current_time + time_step

            query: Query = (
                character_events_query_factory()
                .set_service_id(service_id=service_id)
                .filter("character_id", ",".join((str(c) for c in batch_character_ids)))
                .filter("after", lower_bound)
                .filter("before", upper_bound)
                .filter("type", ",".join(types))
                .limit(max_query_events)
                .limit_per_db(max_query_events)
            )

            res: dict = query.get()

            queries_count += 1

            time.sleep(0.25)

            if "returned" not in res:
                print(res)
                raise Exception("Error !")

            if res["returned"] >= max_query_events:
                raise Exception("Too many !")

            iteration_events: List[dict] = res["characters_event_list"]

            kept_events: int = 0
            for e in filter(
                lambda x: (to_ts >= int(x["timestamp"]) >= from_ts)
                and custom_filter(x),
                iteration_events,
            ):
                events.append(e)
                kept_events += 1

            print(
                f"Kept {kept_events} of {len(iteration_events)} events between {lower_bound} and {upper_bound}"
            )

            current_time = upper_bound

    print(f"Got {len(events)} character events in {queries_count} queries")
    return events


def get_active_outfit_members(
    service_id: str, outfit_tag: str, active_after_ts: int
) -> List[Dict[str, Union[int, str]]]:
    print(f"Getting outfit members")

    query: Query = outfit_members_query_factory().set_service_id(service_id).filter(
        "alias", outfit_tag
    )

    res: dict = query.get()

    if "returned" not in res:
        print(res)
        raise Exception

    assert res["returned"] == 1

    members: List[dict] = [
        {
            "id": int(m["character_id"]),
            "name": m["character"]["name"]["first"],
            "rank": m["rank"],
            "rank_ordinal": int(m["rank_ordinal"]),
            "last_login": int(m["character"]["times"]["last_login"]),
        }
        for m in res["outfit_list"][0]["members"]
    ]

    print(f"Got {len(members)} outfit members")

    active_members: List[Dict[str, str]] = list(
        filter(lambda x: x["last_login"] >= active_after_ts - ACTIVITY_PERIOD, members)
    )

    print(f"Got {len(active_members)} members active after {active_after_ts}")
    return active_members


def generate_outfit_characters_data(
    service_id: str,
    outfit_tag: str,
    time_frames: Iterable[Tuple[int, int]],
    custom_filter: Callable[[dict], bool] = lambda _: True,
):
    member_events: List[dict] = []

    for from_ts, to_ts in time_frames:
        print(f"From {from_ts} to {to_ts}")

        members: List[Dict[str, str]] = get_active_outfit_members(
            service_id=service_id, outfit_tag=outfit_tag, active_after_ts=from_ts
        )

        time_frame_events: List[dict] = (
            get_character_events(
                service_id=service_id,
                character_ids=[m["id"] for m in members],
                from_ts=from_ts,
                to_ts=to_ts,
                custom_filter=custom_filter,
            )
        )

        duplicates: List[Tuple[dict, int]] = [
            (json.loads(v), c)
            for v, c in Counter(
                (json.dumps(e, sort_keys=True) for e in time_frame_events)
            ).items()
            if c > 1
        ]

        print(
            f"""
        {len(time_frame_events)} events
        and {len(duplicates)} duplicates
        of orders {set((c for _, c in duplicates))}
        """
        )

        member_events += time_frame_events

    print(f"Total {len(member_events)} events")

    member_rows: List[dict] = []

    m: dict
    for m in members:
        m_id: int = m["id"]
        name: str = m["name"]
        rank: str = m["rank"]

        m_events: List[dict] = list(
            filter(
                lambda x: m_id
                in {int(x["character_id"]), int(x.get("attacker_character_id", 0))},
                member_events,
            )
        )

        if m_events:
            member_row: dict = {
                "name": name,
                "rank": rank,
                "active_time_hours": round(
                    (15 * 60)
                    * len(
                        set(
                            int(e["timestamp"]) // (15 * 60)
                            for e in filter(
                                lambda x: m_id
                                in {
                                    int(x["character_id"]),
                                    int(x.get("attacker_character_id", 0)),
                                },
                                m_events,
                            )
                        )
                    )
                    / 3600,
                    2,
                ),
                "kills": len(
                    list(
                        filter(
                            lambda x: x.get("table_type") == "kills"
                            and int(x["character_id"]) != m_id
                            and int(x["attacker_character_id"]) == m_id,
                            m_events,
                        )
                    )
                ),
                "vs_kills": len(
                    list(
                        filter(
                            lambda x: x.get("table_type") == "kills"
                            and int(x["character_id"]) != m_id
                            and int(x["attacker_character_id"]) == m_id
                            and Faction(int(x["character"]["faction_id"]))
                            == Faction.VANU_SOVEREIGNTY,
                            m_events,
                        )
                    )
                ),
                "nc_kills": len(
                    list(
                        filter(
                            lambda x: x.get("table_type") == "kills"
                            and int(x["character_id"]) != m_id
                            and int(x["attacker_character_id"]) == m_id
                            and Faction(int(x["character"]["faction_id"]))
                            == Faction.NEW_CONGLOMERATE,
                            m_events,
                        )
                    )
                ),
                "tr_kills": len(
                    list(
                        filter(
                            lambda x: x.get("table_type") == "kills"
                            and int(x["character_id"]) != m_id
                            and int(x["attacker_character_id"]) == m_id
                            and Faction(int(x["character"]["faction_id"]))
                            == Faction.TERRAN_REPUBLIC,
                            m_events,
                        )
                    )
                ),
                "nso_kills": len(
                    list(
                        filter(
                            lambda x: x.get("table_type") == "kills"
                            and int(x["character_id"]) != m_id
                            and int(x["attacker_character_id"]) == m_id
                            and Faction(int(x["character"]["faction_id"]))
                            == Faction.NS_OPERATIVES,
                            m_events,
                        )
                    )
                ),
                "teamkills": len(
                    list(
                        filter(
                            lambda x: x.get("table_type") == "kills"
                            and int(x["character_id"]) != m_id
                            and int(x["attacker_character_id"]) == m_id
                            and Faction(int(x["character"]["faction_id"]))
                            == Faction(int(x["attacker_character"]["faction_id"])),
                            m_events,
                        )
                    )
                ),
                "headshot_kills": len(
                    list(
                        filter(
                            lambda x: x.get("table_type") == "kills"
                            and int(x["character_id"]) != m_id
                            and int(x["attacker_character_id"]) == m_id
                            and int(x["is_headshot"]) == 1,
                            m_events,
                        )
                    )
                ),
                "kill_weapons": json.dumps(
                    Counter(
                        (
                            e["attacker_weapon_item"]["name"]["en"]
                            for e in filter(
                                lambda x: x.get("table_type") == "kills"
                                and int(x["character_id"]) != m_id
                                and int(x["attacker_character_id"]) == m_id
                                and "attacker_weapon_item" in x
                                and "name" in x["attacker_weapon_item"],
                                m_events,
                            )
                        )
                    ).most_common()
                ),
                "kill_vehicles": json.dumps(
                    Counter(
                        (
                            e["vehicle"]["name"]["en"]
                            for e in filter(
                                lambda x: x.get("table_type") == "kills"
                                and int(x["character_id"]) != m_id
                                and int(x["attacker_character_id"]) == m_id
                                and "vehicle" in x,
                                m_events,
                            )
                        )
                    ).most_common()
                ),
                "vehicle_destroys": len(
                    list(
                        filter(
                            lambda x: x.get("event_type") == "VehicleDestroy"
                            and int(x["character_id"]) != m_id
                            and int(x["attacker_character_id"]) == m_id,
                            m_events,
                        )
                    )
                ),
                "deaths": len(
                    list(
                        filter(
                            lambda x: x.get("table_type") == "deaths"
                            and int(x["character_id"]) == m_id
                            and int(x["attacker_character_id"]) != m_id,
                            m_events,
                        )
                    )
                ),
                "vs_deaths": len(
                    list(
                        filter(
                            lambda x: x.get("table_type") == "deaths"
                            and int(x["character_id"]) == m_id
                            and int(x["attacker_character_id"]) != m_id
                            and "attacker_character" in x
                            and Faction(int(x["attacker_character"]["faction_id"]))
                            == Faction.VANU_SOVEREIGNTY,
                            m_events,
                        )
                    )
                ),
                "nc_deaths": len(
                    list(
                        filter(
                            lambda x: x.get("table_type") == "deaths"
                            and int(x["character_id"]) == m_id
                            and int(x["attacker_character_id"]) != m_id
                            and "attacker_character" in x
                            and Faction(int(x["attacker_character"]["faction_id"]))
                            == Faction.NEW_CONGLOMERATE,
                            m_events,
                        )
                    )
                ),
                "tr_deaths": len(
                    list(
                        filter(
                            lambda x: x.get("table_type") == "deaths"
                            and int(x["character_id"]) == m_id
                            and int(x["attacker_character_id"]) != m_id
                            and "attacker_character" in x
                            and Faction(int(x["attacker_character"]["faction_id"]))
                            == Faction.TERRAN_REPUBLIC,
                            m_events,
                        )
                    )
                ),
                "nso_deaths": len(
                    list(
                        filter(
                            lambda x: x.get("table_type") == "deaths"
                            and int(x["character_id"]) == m_id
                            and int(x["attacker_character_id"]) != m_id
                            and "attacker_character" in x
                            and Faction(int(x["attacker_character"]["faction_id"]))
                            == Faction.NS_OPERATIVES,
                            m_events,
                        )
                    )
                ),
                "teamdeaths": len(
                    list(
                        filter(
                            lambda x: x.get("table_type") == "deaths"
                            and int(x["character_id"]) == m_id
                            and int(x["attacker_character_id"]) != m_id
                            and "attacker_character" in x
                            and Faction(int(x["attacker_character"]["faction_id"]))
                            == Faction(int(x["character"]["faction_id"])),
                            m_events,
                        )
                    )
                ),
                "self_kills": len(
                    list(
                        filter(
                            lambda x: x.get("table_type") == "kills"
                            and int(x["character_id"]) == m_id
                            and int(x["attacker_character_id"]) == m_id,
                            m_events,
                        )
                    )
                ),
                "self_deaths": len(
                    list(
                        filter(
                            lambda x: x.get("table_type") == "deaths"
                            and int(x["character_id"]) == m_id
                            and int(x["attacker_character_id"]) == m_id,
                            m_events,
                        )
                    )
                ),
                "death_weapons": json.dumps(
                    Counter(
                        (
                            e["attacker_weapon_item"]["name"]["en"]
                            for e in filter(
                                lambda x: x.get("table_type") == "deaths"
                                and int(x["character_id"]) == m_id
                                and int(x["attacker_character_id"]) != m_id
                                and "attacker_weapon_item" in x
                                and "name" in x["attacker_weapon_item"],
                                m_events,
                            )
                        )
                    ).most_common()
                ),
                "death_vehicles": json.dumps(
                    Counter(
                        (
                            e["vehicle"]["name"]["en"]
                            for e in filter(
                                lambda x: x.get("table_type") == "deaths"
                                and int(x["character_id"]) == m_id
                                and int(x["attacker_character_id"]) != m_id
                                and "vehicle" in x,
                                m_events,
                            )
                        )
                    ).most_common()
                ),
                "facility_captures": len(
                    list(
                        filter(
                            lambda x: x.get("event_type") == "PlayerFacilityCapture",
                            m_events,
                        )
                    )
                ),
                "facility_defends": len(
                    list(
                        filter(
                            lambda x: x.get("event_type") == "PlayerFacilityDefend",
                            m_events,
                        )
                    )
                ),
                "marksman_ribbons": len(
                    list(
                        filter(
                            lambda x: x.get("event_type") == "AchievementEarned"
                            and int(x["achievement_id"]) == 90028,
                            m_events,
                        )
                    )
                ),
                "killstreak_ribbons": len(
                    list(
                        filter(
                            lambda x: x.get("event_type") == "AchievementEarned"
                            and int(x["achievement_id"]) == 90036,
                            m_events,
                        )
                    )
                ),
                "bountycontracts_ribbons": len(
                    list(
                        filter(
                            lambda x: x.get("event_type") == "AchievementEarned"
                            and int(x["achievement_id"]) == 92038,
                            m_events,
                        )
                    )
                ),
                "repair_ribbons": len(
                    list(
                        filter(
                            lambda x: x.get("event_type") == "AchievementEarned"
                            and int(x["achievement_id"]) == 2553,
                            m_events,
                        )
                    )
                ),
                "squadleadership_ribbons": len(
                    list(
                        filter(
                            lambda x: x.get("event_type") == "AchievementEarned"
                            and int(x["achievement_id"]) == 90040,
                            m_events,
                        )
                    )
                ),
                "pointcontrol_ribbons": len(
                    list(
                        filter(
                            lambda x: x.get("event_type") == "AchievementEarned"
                            and int(x["achievement_id"]) == 90030,
                            m_events,
                        )
                    )
                ),
                "piloting_ribbons": len(
                    list(
                        filter(
                            lambda x: x.get("event_type") == "AchievementEarned"
                            and int(x["achievement_id"]) == 2555,
                            m_events,
                        )
                    )
                ),
                "healing_ribbons": len(
                    list(
                        filter(
                            lambda x: x.get("event_type") == "AchievementEarned"
                            and int(x["achievement_id"]) == 2554,
                            m_events,
                        )
                    )
                ),
                "spotter_ribbons": len(
                    list(
                        filter(
                            lambda x: x.get("event_type") == "AchievementEarned"
                            and int(x["achievement_id"]) == 90024,
                            m_events,
                        )
                    )
                ),
                "objectivesupport_ribbons": len(
                    list(
                        filter(
                            lambda x: x.get("event_type") == "AchievementEarned"
                            and int(x["achievement_id"]) == 90032,
                            m_events,
                        )
                    )
                ),
                "savior_ribbons": len(
                    list(
                        filter(
                            lambda x: x.get("event_type") == "AchievementEarned"
                            and int(x["achievement_id"]) == 90021,
                            m_events,
                        )
                    )
                ),
                "reviving_ribbons": len(
                    list(
                        filter(
                            lambda x: x.get("event_type") == "AchievementEarned"
                            and int(x["achievement_id"]) == 2552,
                            m_events,
                        )
                    )
                ),
                "logistics_ribbons": len(
                    list(
                        filter(
                            lambda x: x.get("event_type") == "AchievementEarned"
                            and int(x["achievement_id"]) == 8006,
                            m_events,
                        )
                    )
                ),
                "resupply_ribbons": len(
                    list(
                        filter(
                            lambda x: x.get("event_type") == "AchievementEarned"
                            and int(x["achievement_id"]) == 7995,
                            m_events,
                        )
                    )
                ),
            }

            member_rows.append(member_row)

    member_columns: List[str] = [
        "name",
        "rank",
        "active_time_hours",
        "kills",
        "vs_kills",
        "nc_kills",
        "tr_kills",
        "nso_kills",
        "teamkills",
        "headshot_kills",
        "kill_weapons",
        "kill_vehicles",
        "vehicle_destroys",
        "deaths",
        "vs_deaths",
        "nc_deaths",
        "tr_deaths",
        "nso_deaths",
        "teamdeaths",
        "self_kills",
        "self_deaths",
        "death_weapons",
        "death_vehicles",
        "facility_captures",
        "facility_defends",
        "marksman_ribbons",
        "killstreak_ribbons",
        "bountycontracts_ribbons",
        "repair_ribbons",
        "squadleadership_ribbons",
        "pointcontrol_ribbons",
        "piloting_ribbons",
        "healing_ribbons",
        "spotter_ribbons",
        "objectivesupport_ribbons",
        "savior_ribbons",
        "reviving_ribbons",
        "logistics_ribbons",
        "resupply_ribbons",
    ]

    time_frames_filename_part: str = "_".join(
        "-".join(str(i) for i in e) for e in time_frames
    )

    with open(
        f"output/{slugify(outfit_tag)}_members_{time_frames_filename_part}.csv", "w"
    ) as f:
        writer = csv.DictWriter(f, fieldnames=member_columns)
        writer.writeheader()
        writer.writerows(sorted(member_rows, key=lambda x: x["name"]))
