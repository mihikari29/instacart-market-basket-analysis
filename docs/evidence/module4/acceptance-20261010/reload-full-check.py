from pathlib import Path
import json
import time

from pyspark.sql import functions as F
from src.module4.als import load_model
from src.module4.config import Config
from src.module4.spark import session

output = Path('results/module4/acceptance-20261010')
config = Config.environment(executor_memory='2g', executor_cores=4, total_cores=4, shuffle_partitions=32)
spark = session(config, output / 'reload-full-eventlog')
started = time.perf_counter()
try:
    model = load_model(config.model_path('als-full-20261010'))
    users = model.userFactors.select(F.col('id').alias('user_id'))
    raw = model.recommendForUserSubset(users, 21)
    plan = raw._jdf.queryExecution().executedPlan().toString()
    assert 'CartesianProduct' in plan
    assert 'BroadcastNestedLoopJoin' not in plan
    (output / 'reload-full-plan.txt').write_text(plan)
    row = raw.agg(
        F.count('*').alias('users'),
        F.sum(F.size('recommendations')).alias('candidate_rows'),
        F.min(F.size('recommendations')).alias('min_candidates'),
        F.max(F.size('recommendations')).alias('max_candidates'),
        F.sum(F.exists('recommendations', lambda rec: rec.rating.isNull() | F.isnan(rec.rating) | (F.abs(rec.rating) == float('inf'))).cast('long')).alias('invalid_users'),
    ).first()
    assert row.users == 206209 and row.candidate_rows == 206209 * 21
    assert row.min_candidates == row.max_candidates == 21 and row.invalid_users == 0
    result = {'status': 'passed', 'scope': 'full_saved_model_recommendation_regression', 'read_only': True, 'publication_performed': False, **row.asDict(), 'runtime_ms': round((time.perf_counter()-started)*1000, 3), 'auto_broadcast_join_threshold': spark.conf.get('spark.sql.autoBroadcastJoinThreshold')}
    (output / 'reload-full-check.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2), flush=True)
finally:
    spark.stop()
