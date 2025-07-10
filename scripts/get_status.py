import os
import time
import argparse
import sqlalchemy
import logging
import logging.config
from sqlalchemy import create_engine, Column, String, DateTime, Integer, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker
from globus_sdk import FlowsClient, ClientApp
import datetime

log = logging.getLogger(__name__)

STATES = {
    "SourceTransfer",
    "XpcsBoostCorr",
    "ResultTransferChoice",
    "ResultTransferDoTransfer",
    "ResultTransferSkipTransfer",
    "ResultTransferDone",
    "BoostCorrRuntime",
    "MakeCorrPlots",
    "Publishv2GatherMetadata",
    "Publishv2ChoiceTransfer",
    "Publishv2Transfer",
    "Publishv2SkipTransfer",
    "Publishv2ChoiceIngest",
    "Publishv2Ingest",
    "Publishv2SkipIngest",
    "Publishv2Done",
}

DONE_STATES = {
    "ResultTransferDone",
    "MakeCorrPlots",
    "Publishv2GatherMetadata",
    "Publishv2ChoiceTransfer",
    "Publishv2Transfer",
    "Publishv2SkipTransfer",
    "Publishv2ChoiceIngest",
    "Publishv2Ingest",
    "Publishv2SkipIngest",
    "Publishv2Done",
}

LOGGING = {
    "version": 1,
    "formatters": {
        "basic": {
            "format": "[%(levelname)s - %(process)d - %(created)f] %(funcName)s() %(message)s"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "DEBUG",
            "formatter": "basic",
        }
    },
    "loggers": {
        __name__: {"level": "DEBUG", "handlers": ["console"]},
    },
}


# Set up SQLAlchemy
Base = declarative_base()


class Run(Base):
    __tablename__ = "runs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String, unique=True, nullable=False)
    label = Column(String)
    status = Column(String)
    state_name = Column(String)
    start_time = Column(DateTime)
    completion_time = Column(DateTime)
    last_lookup_time = Column(
        DateTime, default=datetime.datetime.now(datetime.timezone.utc)
    )
    manual_lookup = Column(Boolean, default=False)


class Lock(Base):
    __tablename__ = "locks"
    id = Column(Integer, primary_key=True, autoincrement=True)
    start_time = Column(DateTime, nullable=False)
    completion_time = Column(DateTime)
    aborted = Column(Boolean, default=False)


def get_flows_client():
    client_id = os.environ.get("GLADIER_CLIENT_ID")
    client_secret = os.environ.get("GLADIER_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise Exception(
            "No client credentials specified. Please set GLADIER_CLIENT_ID and GLADIER_CLIENT_SECRET env vars."
        )
    app = ClientApp(
        app_name="XPCSBoost",
        client_id=client_id,
        client_secret=client_secret,
    )
    return FlowsClient(app=app)


def get_or_create_run(
    session,
    run_id,
    label,
    status,
    state_name,
    start_time,
    completion_time=None,
    manual_lookup=False,
):
    if completion_time:
        completion_time = datetime.datetime.fromisoformat(
            completion_time.replace("Z", "+00:00")
        )
    if start_time:
        start_time = datetime.datetime.fromisoformat(start_time.replace("Z", "+00:00"))
    existing = session.query(Run).filter_by(run_id=run_id).first()
    if existing:
        # Update fields if needed
        existing.label = label
        existing.status = status
        existing.start_time = start_time
        existing.state_name = state_name
        existing.completion_time = completion_time
        existing.manual_lookup = manual_lookup
        return existing
    else:
        run_obj = Run(
            run_id=run_id,
            label=label,
            status=status,
            start_time=start_time,
            state_name=state_name,
            completion_time=completion_time,
            manual_lookup=manual_lookup,
        )
        session.add(run_obj)
        return run_obj


def parse_state_name(run_status: dict) -> str:
    """
    Attempt to parse the state name from the flow status info
    """

    if run_status["status"] == "SUCCEEDED":
        # This is considered "Done" from the DM perspective, enough to be marked as finished for DM
        # to start the next run.
        return "ResultTransferDone"
    elif run_status.get("details", {}).get("code") == "FlowStarting":
        return "FlowStarting"

    functions = [
        lambda rs: rs["details"]["action_statuses"][0]["state_name"],
        lambda rs: rs["details"]["details"]["state_name"],
    ]

    for func in functions:
        try:
            return func(run_status)
        except Exception as e:
            pass


def client_list_runs(client, query_params):
    return client.paginated.list_runs(query_params=query_params)


def get_run_list(session, client):
    query_params = dict(orderby="start_time DESC", per_page=50)
    # Fetch recent runs
    for idx, page in enumerate(client_list_runs(client, query_params)):
        if idx >= 4:
            break
        log.debug(f"Fetching page {idx} of run list...")
        for run in page["runs"]:
            run_obj = get_or_create_run(
                session,
                run["run_id"],
                run.get("label"),
                run.get("status"),
                parse_state_name(run),
                run.get("start_time"),
                run.get("completion_time"),
            )
            run_obj.last_lookup_time = datetime.datetime.now(datetime.timezone.utc)


def get_manual_runs(session, client):
    # Fetch runs with manual_lookup=True
    manual_runs = session.query(Run).filter_by(manual_lookup=True).all()
    for run in manual_runs:
        if run.status != "ACTIVE":
            continue
        try:
            run_data = client.get_run(run.run_id)
            run_obj = get_or_create_run(
                session,
                run_data["run_id"],
                run_data.get("label"),
                run_data.get("status"),
                parse_state_name(run_data),
                run_data.get("start_time"),
                run_data.get("completion_time"),
                manual_lookup=True,
            )
            run_obj.last_lookup_time = datetime.datetime.now(datetime.timezone.utc)
        except Exception as e:
            run.status = "ERROR"
            log.info(f"Error fetching run {run.run_id}: {e}")


class LockException(Exception):
    pass



def handle_max_lock_timeouts(session, interval=0):
    max_lock_timeout = interval * 3 or 5

    locks = (
        session.query(Lock)
        .order_by(Lock.start_time.desc())
        .filter(Lock.completion_time.is_(None))
        .all()
    )

    for lock in locks:
        # Check for max lock timeout, which happens when the program exits without the lock being properly released.
        l_start_time = lock.start_time.replace(tzinfo=datetime.timezone.utc)
        lock_time = (
            datetime.datetime.now(datetime.timezone.utc) - l_start_time
        ).total_seconds()
        if not lock.completion_time:
            if lock_time > max_lock_timeout:
                log.warning(f"Lock {lock.id} overstepped max lock time {lock_time}/{max_lock_timeout} without disengaging, closing lock...")
                release_lock(session, lock, aborted=True)

    if len(locks) > 1:    
        locks.sort(key=lambda lck: lck.start_time)
        first_valid_lock = None
        for lock in locks:
            if first_valid_lock:
                release_lock(session, lock, aborted=True)
            elif first_valid_lock is None and not lock.completion_time:
                log.info(f"First valid lock found: {lock}. Releasing all other locks...")
                first_valid_lock = lock


    # Check for lock with the first start time.

    open_locks = [lock for lock in locks if not lock.completion_time]
    log.debug(f"Open Locks: {open_locks}")
    if len(open_locks) > 1:
        log.warning("Multiple live locks currenly active!")

    return open_locks

def check_lock(session, lock: Lock = None, interval: int = 0):
    """
    May switch this to check_lock, and raise exceptions instead.
    """
    locks = handle_max_lock_timeouts(session, interval)

    if lock:
        session.refresh(lock)
        if lock.aborted:
            raise LockException("Lock was auto-released! Aborting!")

    max_lock_timeout = interval * 3 or 15
    # Check if there is a lock
    last_lock = session.query(Lock).order_by(Lock.start_time.desc()).first()
    if last_lock:
        if lock and last_lock.id == lock.id:
            log.debug("Current lock matches this one, we're good!")
            return

        l_start_time = last_lock.start_time.replace(tzinfo=datetime.timezone.utc)
        last_lock_time = (
            datetime.datetime.now(datetime.timezone.utc) - l_start_time
        ).total_seconds()
        if not last_lock.completion_time:
            raise LockException(f"Last lock not completed, waiting...")

        l_completion_time = last_lock.completion_time.replace(tzinfo=datetime.timezone.utc)
        if interval and l_completion_time:
            last_lock_completion_time_seconds = (
                datetime.datetime.now(datetime.timezone.utc) - l_completion_time
            ).total_seconds()

            if last_lock_completion_time_seconds < interval:
                log.debug(
                    f"Interval set to {interval} seconds, must wait {interval - last_lock_completion_time_seconds} seconds before updating database."
                )
                raise LockException("Last lock {last_lock.id} already holds lock and interval not passed, waiting...")
    return


def get_lock(session):
    try:
        check_lock(session)
    except LockException:
        return None
    lock = Lock(start_time=datetime.datetime.now(datetime.timezone.utc))
    session.add(lock)
    session.commit()
    log.debug(f"Lock acquired: {lock.id}")
    return lock


def release_lock(session, lock, aborted=False):
    session.refresh(lock)
    lock.completion_time = datetime.datetime.now(datetime.timezone.utc)
    lock.aborted = aborted
    session.commit()
    if aborted:
        log.warning(f"Lock force released due to simultaneous locks: {lock.id}")
    else:
        log.debug(f"Lock released: {lock.id}")
    


def fetch_and_cache_runs(session):
    client = get_flows_client()
    import random

    time.sleep(random.randint(1, 10) / 100)
    lock = get_lock(session)
    if not lock:
        return
    time.sleep(random.randint(1, 10) / 100)
    try:
        check_lock(session, lock)
        get_run_list(session, client)
        get_manual_runs(session, client)
    except LockException as e:
        log.error(e)
    finally:
        release_lock(session, lock)


def get_run_with_session(session, run_id: str, interval: int = 0) -> Run:
    fetch_and_cache_runs(session)
    run = session.query(Run).filter_by(run_id=run_id).first()
    if not run:
        try:
            get_or_create_run(
                session, run_id, None, "ACTIVE", None, None, manual_lookup=True
            )
            session.commit()
        except (sqlalchemy.exc.OperationalError, sqlalchemy.exc.IntegrityError) as err:
            # These errors typically happen when two duplicate processes attempt to get the same run id.
            # They should be harmless errors, and if this fails it should happen again the next time around.
            log.debug(str(err))
            log.debug(f"Database Error, waiting until next time to setup run.")


def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--run_id", help="The automate flow instance(run) to check.", required=True
    )
    parser.add_argument(
        "--step",
        help="The inside the flow execution to check",
        default="ResultTransferDone",
    )
    parser.add_argument(
        "--interval", help="Interval between checking statuses", type=int, default=5
    )
    parser.add_argument(
        "--max-wait",
        help="Maixmum time to wait for a run before exiting",
        type=int,
        # 1 week
        default=60 * 60 * 24 * 7,
    )
    parser.add_argument(
        "--verbose",
        help="Show more verbose logging",
        action='store_true',
    )
    parser.add_argument(
        "--quiet",
        help="Show less logging",
        action='store_true',
    )

    parser.add_argument(
        "--filename",
        help="The filename to use for the cache",
        default=os.path.abspath("run_status_cache.sqlite"),
    )
    args = parser.parse_args()
    return args


def get_run(run_id, interval, cache_filename, debug_logging=False):
    engine = create_engine(f"sqlite:///{cache_filename}")
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)

    if debug_logging:
        LOGGING["handlers"]["console"]["level"] = "INFO"
        logging.config.dictConfig(LOGGING)
    with SessionLocal() as session:
        return get_run_with_session(session, run_id, interval)


if __name__ == "__main__":
    args = arg_parse()
    start_time = datetime.datetime.now()
    if args.verbose:
        log_level = "DEBUG"
    elif args.quiet:
        log_level = "CRITICAL"
    else:
        log_level = "INFO"
    LOGGING["handlers"]["console"]["level"] = log_level
    logging.config.dictConfig(LOGGING)

    log.debug(f"Writing to {args.filename}")

    while True:
        if datetime.datetime.now() - start_time > datetime.timedelta(
            seconds=args.max_wait
        ):
            log.info(f"Max wait time of {args.max_wait} seconds reached. Exiting.")
            break
        run = get_run(args.run_id, args.interval, args.filename)

        if run and run.state_name and run.state_name not in STATES:
            log.warning(
                f"Warning: Run {args.run_id} state name {run.state_name} not in STATES. Early exit will be unavailable."
            )

        if run:
            if (
                run.status in ["SUCCEEDED", "FAILED", "ERROR"]
                or run.state_name in DONE_STATES
            ):
                break
        # Pause for 0.2 seconds for DB updates
        time.sleep(0.2)

    print(f"Run {args.run_id} status: {run.status}")
