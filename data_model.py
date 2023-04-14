from typing import List
from typing import Optional
from sqlalchemy import ForeignKey
from sqlalchemy import Integer, String, DateTime, Enum
from sqlalchemy.sql import func
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from datetime import datetime
import enum

class Base(DeclarativeBase):
    pass

class TransactionType(enum.Enum):
    debit = "debit"
    credit = "credit"

class Transaction(Base):
    __tablename__ = "transaction"
    id: Mapped[int] = mapped_column(primary_key=True)
    booking_date: Mapped[datetime] = mapped_column(DateTime(timezone=False))
    value_date: Mapped[datetime] = mapped_column(DateTime(timezone=False))
    amount: Mapped[float]
    tr_type: Mapped[TransactionType]

    transaction_details: Mapped[List["TransactionDetail"]] = relationship(
        back_populates="transaction", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"Transasction(id={self.id!r}, amount={self.amount!r}, transaction type={self.tr_type!r}, value date={self.value_date!r})"

class TransactionDetailType(Base):
    __tablename__ = "transaction_detail_type"
    id: Mapped[int] = mapped_column(primary_key=True)
    description: Mapped[str]
    label: Mapped[str] = mapped_column(String(255), unique=True)

    transaction_details: Mapped[List["TransactionDetail"]] = relationship(
        back_populates="transaction_detail_type"
    )
    def __repr__(self) -> str:
        return f"TransactionDetailType(id={self.id!r}, label={self.label!r}, description={self.description!r})"


class TransactionDetail(Base):
    __tablename__ = "transaction_detail"
    id: Mapped[int] = mapped_column(primary_key=True)
    transaction_id: Mapped[int] = mapped_column(ForeignKey("transaction.id"))
    transaction_detail_type_id: Mapped[int] = mapped_column(ForeignKey("transaction_detail_type.id"))
    description: Mapped[str]

    transaction: Mapped["Transaction"] = relationship(back_populates="transaction_details")
    transaction_detail_type: Mapped["TransactionDetailType"] = relationship(back_populates="transaction_details")

    def __repr__(self) -> str:
        return f"TransactionDetail(id={self.id!r}, description={self.description!r}, transaction_id={self.transaction_id!r}, transaction_detail_type_id={self.transaction_detail_type_id!r})"
    

def create_tables():
    from db import engine
    Base.metadata.create_all(engine)

if __name__ == "__main__":
    create_tables()