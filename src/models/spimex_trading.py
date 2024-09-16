import uuid
from datetime import datetime, date
from sqlalchemy import UUID, CheckConstraint
from sqlalchemy.orm import Mapped, mapped_column

from database.db import Base


class SpimexTradingResults(Base):

    __tablename__ = "spimex_trading_results"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    exchange_product_id: Mapped[str]
    exchange_product_name: Mapped[str]
    oil_id: Mapped[str]
    delivery_basis_id: Mapped[str]
    delivery_basis_name: Mapped[str]
    delivery_type_id: Mapped[str]
    volume: Mapped[int]
    total: Mapped[int]
    count: Mapped[int]
    date: Mapped[date]
    created_on: Mapped[datetime] = mapped_column(default=datetime.now)
    updated_on: Mapped[datetime] = mapped_column(
        default=datetime.now, onupdate=datetime.now
    )

    __table_args__ = (
        CheckConstraint("volume >= 0", name="check_volume_positive"),
        CheckConstraint("total >= 0", name="check_total_positive"),
        CheckConstraint("count >= 0", name="check_count_positive"),
    )
