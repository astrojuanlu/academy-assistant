import logging
import os
from time import sleep

import polars as pl
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import structlog

logger = structlog.get_logger()


def get_all_channels(*, client, cooloff=1):
    cursor = None
    while True:
        try:
            result = client.conversations_list(
                exclude_archived=True,
                types="public_channel,private_channel",
                cursor=cursor,
            )
        except SlackApiError:
            logger.exception("Request errored")
        if not result["ok"]:
            logger.error("Request was not OK", error_info=result["error"])
        else:
            logger.debug("Request succeeded")
            yield from result["channels"]
            if cursor := result["response_metadata"].get("next_cursor"):
                sleep(cooloff)
                continue
            else:
                break


def user_ids_in_channel(channel_id, *, client, cooloff=1):
    cursor = None
    while True:
        try:
            result = client.conversations_members(channel=channel_id, cursor=cursor)
        except SlackApiError:
            logger.exception("Request errored")
        if not result["ok"]:
            logger.error("Request was not OK", error_info=result["error"])
        else:
            logger.debug("Request succeeded")
            yield from result["members"]
            if cursor := result["response_metadata"].get("next_cursor"):
                sleep(cooloff)
                continue
            else:
                break


def user_from_email(email, *, client):
    try:
        result = client.users_lookupByEmail(email=email)
    except SlackApiError as e:
        logger.error("Request failed", error_msg=e.response["error"])
        return None
    else:
        logger.debug("Request succeeded", user_info=result["user"])
        assert result["ok"]
        return result["user"]


def main():
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO)
    )

    registered_participants = (
        pl.read_excel(os.environ["ROSTER_FILENAME"])
        .filter(pl.col("Transcript Status") == "Registered")
        .select(
            (pl.col("User First Name") + " " + pl.col("User Last Name")).alias("name"),
            pl.col("User Email").alias("email"),
        )
        .to_dicts()
    )

    client = WebClient(token=os.environ["SLACK_BOT_TOKEN"])
    channel_id = os.environ["CHANNEL_ID"]

    user_ids_already_in_channel = user_ids_in_channel(channel_id, client=client)

    pending_users = []
    for participant in registered_participants:
        user = user_from_email(participant["email"], client=client)
        if not user:
            logger.warning(
                "Participant has not joined Slack yet",
                email=participant["email"],
                name=participant["name"],
            )
        else:
            logger.debug("Participant has user in Slack", user_info=user)
            if user["id"] in user_ids_already_in_channel:
                logger.info(
                    "User already in channel ✔",
                    email=participant["email"],
                    name=participant["name"],
                )
            else:
                logger.info(
                    "Adding user to list of pending invites",
                    email=participant["email"],
                    name=participant["name"],
                )
                pending_users.append(user["id"])

    logger.info("Sending invites", pending_users=pending_users)
    client.conversations_invite(channel=channel_id, users=",".join(pending_users))


if __name__ == "__main__":
    main()
