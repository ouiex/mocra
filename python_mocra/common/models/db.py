from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, JSON, DateTime, Text
from sqlalchemy.orm import relationship, Mapped, mapped_column
from sqlalchemy.sql import func
from common.db import Base
from typing import Optional, List, Any
from datetime import datetime

class Account(Base):
    __tablename__ = 'account'
    # __table_args__ = {"schema": "base"}
    
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True, index=True)
    modules: Mapped[List[str]] = mapped_column(JSON, default=list) # TEXT[] in PG, but likely JSON is safer for portable
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    config: Mapped[dict] = mapped_column(JSON, default=dict)
    priority: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    # Relationships
    platform_relations: Mapped[List["RelAccountPlatform"]] = relationship(back_populates="account")
    module_relations: Mapped[List["RelModuleAccount"]] = relationship(back_populates="account")

class Platform(Base):
    __tablename__ = 'platform'
    # __table_args__ = {"schema": "base"}
    
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True, index=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    base_url: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    config: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    # Relationships
    account_relations: Mapped[List["RelAccountPlatform"]] = relationship(back_populates="platform")
    module_relations: Mapped[List["RelModulePlatform"]] = relationship(back_populates="platform")

class Module(Base):
    __tablename__ = 'module'
    # __table_args__ = {"schema": "base"}
    
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True, index=True)
    version: Mapped[int] = mapped_column(Integer, default=1)
    cron: Mapped[dict] = mapped_column(JSON, default=dict)
    config: Mapped[dict] = mapped_column(JSON, default=dict)
    priority: Mapped[int] = mapped_column(Integer, default=0)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    account_relations: Mapped[List["RelModuleAccount"]] = relationship(back_populates="module")
    platform_relations: Mapped[List["RelModulePlatform"]] = relationship(back_populates="module")

class RelAccountPlatform(Base):
    __tablename__ = 'rel_account_platform'
    # __table_args__ = {"schema": "base"}
    
    account_id: Mapped[int] = mapped_column(ForeignKey('account.id'), primary_key=True)
    platform_id: Mapped[int] = mapped_column(ForeignKey('platform.id'), primary_key=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    config: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    account: Mapped["Account"] = relationship(back_populates="platform_relations")
    platform: Mapped["Platform"] = relationship(back_populates="account_relations")

class RelModuleAccount(Base):
    __tablename__ = 'rel_module_account'
    # __table_args__ = {"schema": "base"}
    
    module_id: Mapped[int] = mapped_column(ForeignKey('module.id'), primary_key=True)
    account_id: Mapped[int] = mapped_column(ForeignKey('account.id'), primary_key=True)
    priority: Mapped[int] = mapped_column(Integer, default=0)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    config: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    module: Mapped["Module"] = relationship(back_populates="account_relations")
    account: Mapped["Account"] = relationship(back_populates="module_relations")

class RelModulePlatform(Base):
    __tablename__ = 'rel_module_platform'
    # __table_args__ = {"schema": "base"}
    
    module_id: Mapped[int] = mapped_column(ForeignKey('module.id'), primary_key=True)
    platform_id: Mapped[int] = mapped_column(ForeignKey('platform.id'), primary_key=True)
    priority: Mapped[int] = mapped_column(Integer, default=0)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    config: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    module: Mapped["Module"] = relationship(back_populates="platform_relations")
    platform: Mapped["Platform"] = relationship(back_populates="module_relations")
