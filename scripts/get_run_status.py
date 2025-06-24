import os
import time
import argparse
from sqlalchemy import create_engine, Column, String, DateTime, Integer
from sqlalchemy.orm import declarative_base, sessionmaker
from globus_sdk import FlowsClient, ClientApp
import datetime

STATES = {
    'XpcsBoostCorr', 'ResultTransferChoice', 'ResultTransferDoTransfer', 
    'ResultTransferSkipTransfer', 'ResultTransferDone', 'BoostCorrRuntime', 
    'MakeCorrPlots', 'Publishv2GatherMetadata', 'Publishv2ChoiceTransfer', 
    'Publishv2Transfer', 'Publishv2SkipTransfer', 'Publishv2ChoiceIngest', 
    'Publishv2Ingest', 'Publishv2SkipIngest', 'Publishv2Done'
}

DONE_STATES = {
    'ResultTransferDone', 'MakeCorrPlots', 'Publishv2GatherMetadata', 'Publishv2ChoiceTransfer', 
    'Publishv2Transfer', 'Publishv2SkipTransfer', 'Publishv2ChoiceIngest', 
    'Publishv2Ingest', 'Publishv2SkipIngest', 'Publishv2Done'
}

# Set up SQLAlchemy
Base = declarative_base()
engine = create_engine("sqlite:///run_status_cache.sqlite")
SessionLocal = sessionmaker(bind=engine)


class Run(Base):
    __tablename__ = "runs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String, unique=True, nullable=False)
    label = Column(String)
    status = Column(String)
    state_name = Column(String)
    start_time = Column(DateTime)
    completion_time = Column(DateTime)
    last_lookup_time = Column(DateTime, default=datetime.datetime.now(datetime.timezone.utc))
    manual_lookup = Column(String, default=False)


class Lock(Base):
    __tablename__ = "locks"
    id = Column(Integer, primary_key=True, autoincrement=True)
    start_time = Column(DateTime, nullable=False)
    completion_time = Column(DateTime)


Base.metadata.create_all(engine)


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


def get_or_create_run(session, run_id, label, status, state_name, start_time, completion_time=None, manual_lookup=False):
    if completion_time:
        completion_time = datetime.datetime.fromisoformat(completion_time.replace("Z", "+00:00"))
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
    try:
        return run_status["details"]["state_name"]
    except Exception as e:
        # print(f"Error parsing state name: {e}")
        # from pprint import pprint
        # pprint(run_status)
        return None


def fetch_and_cache_runs():
    client = get_flows_client()
    query_params = dict(orderby="start_time DESC", per_page=50)
    with SessionLocal() as session:
        lock = Lock(start_time=datetime.datetime.now(datetime.timezone.utc))
        session.add(lock)
        session.commit()
        try:
            # Fetch recent runs
            for idx, page in enumerate(
                client.paginated.list_runs(query_params=query_params)
            ):
                if idx >= 4:
                    break
                print(f"Found {len(page['runs'])} runs")
                for run in page["runs"]:
                    run_obj = get_or_create_run(
                        session, 
                        run["run_id"], 
                        run.get("label"), 
                        run.get("status"), 
                        parse_state_name(run),
                        run.get("start_time"), 
                        run.get("completion_time")
                    )
                    run_obj.last_lookup_time = datetime.datetime.now(datetime.timezone.utc)
            
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
                        parse_state_name(run),
                        run_data.get("start_time"),
                        run_data.get("completion_time"),
                        manual_lookup=True
                    )
                    run_obj.last_lookup_time = datetime.datetime.now(datetime.timezone.utc)
                except Exception as e:
                    print(f"Error fetching run {run.run_id}: {e}")
            
            session.commit()
        finally:
            lock.completion_time = datetime.datetime.now(datetime.timezone.utc)
            session.commit()


def check_update_runs(interval: int = 0):
    with SessionLocal() as session:
        # Check if there is a lock and the last one is older than 5 minutes
        lock = session.query(Lock).order_by(Lock.start_time.desc()).first()
        if lock:
            last_lock_time = (datetime.datetime.now() - lock.start_time).total_seconds()
            print(f"Lock Completion Time: {lock.completion_time}")
            if not lock.completion_time and last_lock_time < 300:
                print("Lock already acquired and has not expired. Skipping update.")
                return

            if interval and lock.completion_time:
                last_lock_completion_time = (datetime.datetime.now() - lock.completion_time).total_seconds()
                if last_lock_completion_time > interval:
                    print(f"Interval set to {interval} seconds, must wait {last_lock_completion_time - interval} seconds before updating database.")
                    return
        
        print("No lock found or lock is older than 5 minutes. Updating database...")
        fetch_and_cache_runs()


def get_run(run_id: str, interval: int = 0) -> Run:
    check_update_runs(interval=interval)

    with SessionLocal() as session:
        run = session.query(Run).filter_by(run_id=run_id).first()
        if not run:
            lock = session.query(Lock).order_by(Lock.start_time.desc()).first()
            if lock:
                get_or_create_run(session, run_id, None, None, None, None, manual_lookup=True)
                print(f"Run {run_id} not found in database. Added to manual_lookup.")
        return run


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
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = arg_parse()
    start_time = datetime.datetime.now()
    while True:
        if datetime.datetime.now() - start_time > datetime.timedelta(seconds=args.max_wait):
            print(f"Max wait time of {args.max_wait} seconds reached. Exiting.")
            break
        run = get_run(args.run_id, args.interval)

        if run.state_name and run.state_name not in STATES:
            print(f"Warning: Run {args.run_id} state name {run.state_name} not in STATES. Early exit will be unavailable.")

        if run.status in ["SUCCEEDED", "FAILED"] or run.state_name in DONE_STATES:
            break
        # Pause for 0.2 seconds for DB updates
        time.sleep(.2)

    print(f"Run {args.run_id} status: {run.status}")
