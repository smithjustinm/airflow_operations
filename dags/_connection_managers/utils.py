"""Utility function for managing Airflow connections."""

from airflow.models import Connection
from airflow.settings import Session
from airflow.utils.session import provide_session


@provide_session
def update_airflow_connection(new_conn: Connection, session: Session, replace: bool = True):
    """Update or create a connection in Airflow.

    Args:
        new_conn (Connection): Connection object to update or create.
        session (Session): Airflow session.
        replace (bool): Whether to replace an existing connection.

    Returns:
        None
    """
    print(session)

    # noinspection PyUnresolvedReferences
    existing_conn: Connection = (
        session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).first()
    )

    if existing_conn and replace:
        # Update the existing connection
        existing_conn.conn_id = new_conn.conn_id
        existing_conn.conn_type = new_conn.conn_type
        existing_conn.host = new_conn.host
        existing_conn.login = new_conn.login
        existing_conn.set_password(new_conn.password)
        existing_conn.extra = new_conn.extra
        session.add(existing_conn)
        session.commit()
    else:
        # Store new connection
        session.add(new_conn)
        session.commit()
