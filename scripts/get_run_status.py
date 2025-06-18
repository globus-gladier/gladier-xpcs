import os
from sqlalchemy import create_engine, Column, String, DateTime, Integer
from sqlalchemy.orm import declarative_base, sessionmaker
from globus_sdk import FlowsClient, ClientApp
import datetime

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
    start_time = Column(DateTime)
    completion_time = Column(DateTime)


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


def fetch_and_cache_runs():
    client = get_flows_client()
    query_params = dict(orderby="start_time DESC", limit=200, per_page=50)
    with SessionLocal() as session:
        lock = Lock(start_time=datetime.datetime.now(datetime.timezone.utc))
        session.add(lock)
        session.commit()
        try:
            for idx, page in enumerate(
                client.paginated.list_runs(query_params=query_params)
            ):
                if idx >= 4:
                    break
                print(f"Found {len(page['runs'])} runs")
                for run in page["runs"]:
                    run_obj = Run(
                        run_id=run["run_id"],
                        label=run.get("label"),
                        status=run.get("status"),
                        start_time=datetime.datetime.fromisoformat(
                            run["start_time"].replace("Z", "+00:00")
                        )
                        if run.get("start_time")
                        else None,
                        completion_time=datetime.datetime.fromisoformat(
                            run["completion_time"].replace("Z", "+00:00")
                        )
                        if run.get("completion_time")
                        else None,
                    )
                    existing = (
                        session.query(Run).filter_by(run_id=run["run_id"]).first()
                    )
                    if existing:
                        existing.label = run_obj.label
                        existing.status = run_obj.status
                        existing.start_time = run_obj.start_time
                        existing.completion_time = run_obj.completion_time
                    else:
                        session.add(run_obj)
            session.commit()
        finally:
            lock.completion_time = datetime.datetime.now(datetime.timezone.utc)
            session.commit()


def check_update_runs():
    with SessionLocal() as session:
        # Check if there is a lock and the last one is older than 5 minutes
        lock = session.query(Lock).order_by(Lock.start_time.desc()).first()

        last_lock_time = (datetime.datetime.now() - lock.start_time).total_seconds()
        if lock and (not lock.completion_time or last_lock_time > 300):
            print("Lock already acquired and has not expired. Skipping update.")
            return

        print("No lock found or lock is older than 5 minutes. Updating database...")
        fetch_and_cache_runs()


def get_run(run_id: str) -> Run:
    check_update_runs()

    with SessionLocal() as session:
        run = session.query(Run).filter_by(run_id=run_id).first()
        if not run:
            print(f"Run {run_id} not found in database.")
        return run


if __name__ == "__main__":
    get_run(None)
