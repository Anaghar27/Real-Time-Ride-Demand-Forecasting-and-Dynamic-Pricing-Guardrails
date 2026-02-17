-- Query template: slice-level KPI rollup for a training run
SELECT
    run_id,
    model_name,
    slice_name,
    AVG(mae) AS mae,
    AVG(rmse) AS rmse,
    AVG(wape) AS wape,
    AVG(smape) AS smape,
    SUM(row_count) AS rows
FROM training_slice_metrics
WHERE run_id = :run_id
GROUP BY run_id, model_name, slice_name
ORDER BY model_name, slice_name;

-- Query template: baseline vs candidate sparse-zone comparison
SELECT
    b.model_name AS baseline_model,
    c.model_name AS candidate_model,
    b.wape AS baseline_sparse_wape,
    c.wape AS candidate_sparse_wape,
    (c.wape - b.wape) AS sparse_wape_delta
FROM training_slice_metrics b
JOIN training_slice_metrics c
  ON b.run_id = c.run_id
 AND b.slice_name = 'sparse_zones'
 AND c.slice_name = 'sparse_zones'
WHERE b.run_id = :run_id
  AND b.model_name = :baseline_model
  AND c.model_name = :candidate_model;
