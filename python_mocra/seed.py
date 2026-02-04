import asyncio
import logging
from common.db import Database
from common.models.db import Account, Platform, Module, RelAccountPlatform, RelModuleAccount, RelModulePlatform
from common.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("seed")

async def seed():
    logger.info("Initializing DB...")
    # Initialize DB (creates tables if missing due to our patch)
    db = await Database.init(settings.database.url)
    
    async with db.session_factory() as session:
        logger.info("Seeding data...")
        
        # Check if data exists
        from sqlalchemy import select
        res = await session.execute(select(Account).where(Account.name == "user_standalone"))
        if res.scalar_one_or_none():
            logger.info("Data already exists.")
            return

        # Create Entities
        acc = Account(name="user_standalone", enabled=True)
        session.add(acc)
        
        plt = Platform(name="demo_platform", enabled=True, base_url="https://httpbin.org")
        session.add(plt)
        
        mod = Module(name="demo_spider", enabled=True, version=1, cron={"enable": False})
        session.add(mod)
        
        await session.flush() # get IDs
        
        # Create Relations
        rel_ap = RelAccountPlatform(account_id=acc.id, platform_id=plt.id, enabled=True)
        session.add(rel_ap)
        
        rel_ma = RelModuleAccount(module_id=mod.id, account_id=acc.id, enabled=True)
        session.add(rel_ma)
        
        rel_mp = RelModulePlatform(module_id=mod.id, platform_id=plt.id, enabled=True)
        session.add(rel_mp)
        
        await session.commit()
        logger.info("Seeding complete.")

if __name__ == "__main__":
    asyncio.run(seed())
