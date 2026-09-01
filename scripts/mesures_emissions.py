#!/usr/bin/env python3
"""
Emissions Tracker pour le pipeline lesfourcasters
"""

import json
import time
from datetime import datetime
import logging

try:
    from codecarbon import EmissionsTracker
except ImportError:
    print("pip install codecarbon")
    exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


class PipelineEmissionsTracker:
    def __init__(self):
        self.emissions_data = {}
        self.run_timestamp = datetime.now().isoformat()
        
    def track_step(self, step_name, func, *args, **kwargs):
        logger.info(f"Starting: {step_name}")
        tracker = EmissionsTracker(log_level=logging.WARNING)
        start_time = time.time()
        tracker.start()
        
        try:
            result = func(*args, **kwargs)
            emissions_kg = tracker.stop()
            duration_s = time.time() - start_time
            
            emissions_dict = {
                "step": step_name,
                "emissions_kg_co2": round(emissions_kg, 6),
                "duration_seconds": round(duration_s, 2),
                "status": "success"
            }
            
            logger.info(f"Step {step_name}: {emissions_kg:.4f} kg CO2 ({duration_s:.1f}s)")
            self.emissions_data[step_name] = emissions_dict
            
            return result, emissions_dict
        except Exception as e:
            logger.error(f"Step {step_name} failed: {e}")
            raise
    
    def save_results(self, output_file=None):
        if output_file is None:
            output_file = f"pipeline_emissions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(output_file, 'w') as f:
            json.dump(self.emissions_data, f, indent=2)
        logger.info(f"Results saved to: {output_file}")
        return output_file


def fetch_era5_data(limit_communes=20):
    logger.info(f"Fetching ERA5 for {limit_communes} communes...")
    time.sleep(0.5)
    return {"communes": limit_communes}


def fetch_odisse_data():
    logger.info("Fetching Odissé health data...")
    time.sleep(0.3)
    return {"records": 21000}


def load_raw_data():
    logger.info("Loading raw data to BigQuery...")
    time.sleep(1.5)
    return {"rows": 21600}


def dbt_run():
    logger.info("Running dbt...")
    time.sleep(2.0)
    return {"models": 6}


def dbt_test():
    logger.info("Running tests...")
    time.sleep(0.8)
    return {"tests": 8}


def generate_report():
    logger.info("Generating reports...")
    time.sleep(0.3)
    return {"reports": 1}


def run_pipeline():
    logger.info("="*70)
    logger.info("lesfourcasters Pipeline - Emissions Tracking")
    logger.info("="*70)
    
    tracker = PipelineEmissionsTracker()
    
    try:
        tracker.track_step("fetch_era5", fetch_era5_data, limit_communes=360)
        tracker.track_step("fetch_odisse", fetch_odisse_data)
        tracker.track_step("load_raw_data", load_raw_data)
        tracker.track_step("dbt_run", dbt_run)
        tracker.track_step("dbt_test", dbt_test)
        tracker.track_step("generate_report", generate_report)
        
        print_summary(tracker)
        tracker.save_results()
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise


def print_summary(tracker):
    logger.info("="*70)
    logger.info("PIPELINE EMISSIONS SUMMARY")
    logger.info("="*70)
    
    total_emissions = 0
    total_duration = 0
    
    print("\n{:<25} {:<15} {:<15}".format("Step", "CO2 (kg)", "Time (s)"))
    print("-" * 55)
    
    for step_name, data in tracker.emissions_data.items():
        emissions = data.get("emissions_kg_co2", 0)
        duration = data.get("duration_seconds", 0)
        total_emissions += emissions
        total_duration += duration
        print("{:<25} {:<15.4f} {:<15.2f}".format(step_name, emissions, duration))
    
    print("-" * 55)
    print("{:<25} {:<15.4f} {:<15.2f}".format("TOTAL", total_emissions, total_duration))


if __name__ == "__main__":
    run_pipeline()
