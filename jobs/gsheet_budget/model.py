# mypy: ignore-errors
import os

from sqlalchemy import (
    CheckConstraint,
    Column,
    Date,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    create_engine,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

Base = declarative_base()


class Account(Base):
    __tablename__ = "accounts"
    id = Column(Integer, primary_key=True)
    account_name = Column(String(31), nullable=False)
    starting_amount = Column(Float, nullable=False, default=0.0)
    description = Column(String(511))
    is_open = Column(Integer, CheckConstraint("is_open in (0, 1)"), default=1)


class Withdrawal(Base):
    __tablename__ = "withdrawals"
    id = Column(Integer, primary_key=True)
    withdrawal_date = Column(Date, nullable=False)
    amount = Column(Float, nullable=False)
    account_id = Column(Integer, ForeignKey("accounts.id"))
    account = relationship(Account)
    category = Column(String(31), nullable=False)
    notes = Column(String(255), nullable=True)
    additional_notes = Column(Text, nullable=True)


class Deposit(Base):
    __tablename__ = "deposits"
    id = Column(Integer, primary_key=True)
    deposit_date = Column(Date, nullable=False)
    amount = Column(Float, nullable=False)
    account_id = Column(Integer, ForeignKey("accounts.id"))
    account = relationship(Account)
    category = Column(String(31), nullable=False)
    notes = Column(String(255), nullable=True)
    additional_notes = Column(Text, nullable=True)


class Budget(Base):
    __tablename__ = "budgets"
    id = Column(Integer, primary_key=True)
    budget_month = Column(Date, nullable=False)


class BudgetItem(Base):
    __tablename__ = "budget_items"
    id = Column(Integer, primary_key=True)
    budget_id = Column(Integer, ForeignKey("budgets.id"))
    budget = relationship(Budget)
    planned_amount = Column(Float, nullable=False)
    category = Column(String(31), nullable=False)
    additional_notes = Column(Text, nullable=True)


def init_db():
    engine = create_engine(
        f"postgresql+psycopg2://{os.environ['BUDGET_DATABASE_USER']}:{os.environ['BUDGET_DATABASE_PASSWORD']}"
        f"@{os.environ['DATABASE_HOST']}/{os.environ['BUDGET_DATABASE_DB']}"
    )
    Base.metadata.create_all(engine)
    db_session = sessionmaker(bind=engine)
    return db_session()
