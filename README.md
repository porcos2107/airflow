# airflow
#### branch_python_operator DAGの実行コマンド

```bash
gcloud beta composer environments run gcpug-shonan --location="us-central1" trigger_dag -- "branch_python_operator" --conf '{"word": "hoge"}'
```