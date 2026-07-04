from datetime import datetime

def hello_git_sync(**context):
    print("=" * 50)
    print("DAG carregada via git-sync!")
    print(f"Horário execução: {datetime.now()}")
    print(f"DAG ID: {context['dag'].dag_id}")
    print(f"Run ID: {context['run_id']}")
    print("=" * 50)
    return "git-sync funcionando!"
