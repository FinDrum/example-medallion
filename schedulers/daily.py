from apscheduler.triggers.cron import CronTrigger
from findrum.interfaces import Scheduler
from datetime import datetime

class DailyScheduler(Scheduler):
    
    def register(self, scheduler):
        
        hour = self.config.get("hour", 0)
        minute = self.config.get("minute", 0)
        start_date_str = self.config.get("start_date")
        
        trigger_kwargs = {
            "hour": hour,
            "minute": minute
        }        

        if start_date_str:
            try:
                trigger_kwargs["start_date"] = datetime.fromisoformat(start_date_str)
            except Exception as e:
                raise ValueError(f"Invalid start_date format: {start_date_str}") from e

        trigger = CronTrigger(**trigger_kwargs)

        scheduler.add_job(
            func=self._run_pipeline,
            trigger=trigger,
            id=self.pipeline_path,
            name=f"{self.pipeline_path} @ {hour:02d}:{minute:02d}",
            replace_existing=True
        )