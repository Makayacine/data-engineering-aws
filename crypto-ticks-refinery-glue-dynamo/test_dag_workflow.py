"""DagBag check for dag-glue-workflow.py. Needs Airflow; needs no AWS and no network.

A DAG has no ``--self-check``: it is not a program that runs, it is a graph a scheduler parses,
and every way it can be wrong is a parse or a wiring error rather than an arithmetic one. So
what this asserts is that the file becomes a DAG at all, that the graph is the one the module
docstring draws, and that the two properties the whole design leans on are actually true of the
operator rather than assumed of it.

Measured against Airflow 2.9.3 with ``apache-airflow-providers-amazon`` 8.25.0, in the official
``apache/airflow`` image. The reference lab's DAG parses there too -- the common claim that its
``airflow.operators.dummy_operator`` import breaks parsing on 2.4+ turns out to be false, it is
a deprecation shim and Lab 2's DAG loads with no import errors. What that DAG would fail is
``test_every_glue_task_waits_for_completion``: its hand-rolled poller returns success on a
``FAILED`` job.

Run it where Airflow is installed -- the scheduler box, or the container the README names::

    python test_dag_workflow.py

That is the invocation this file was verified with, and the only one it needs: the checks are
plain module-level functions with no fixtures and no conftest, so the ``__main__`` block below
runs them itself. They are named ``test_*`` so pytest collects them too on a box that has it;
the official ``apache/airflow`` image does not, and has no network to fetch it.
"""

import os

from airflow.models import DagBag
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

DAG_ID = 'crypto_ticks_refinery'
DAG_FILE = 'dag-glue-workflow.py'

# The graph the module docstring draws, as {task_id: set of direct downstream task ids}.
EXPECTED_EDGES = {
    'validate_raw_ticks': {'check_validation'},
    'check_validation': {'ingest_bars', 'end_dag'},
    'ingest_bars': {'refine_path1', 'refine_path2', 'refine_path3'},
    'refine_path1': {'load_dynamo'},
    'refine_path2': {'load_dynamo'},
    'refine_path3': {'load_dynamo'},
    'load_dynamo': set(),
    'end_dag': set(),
}


def load_dag():
    """Parse the DAG file itself, and fail loudly on any import error.

    The file, not the folder. This project has a flat layout, so the folder also holds four
    Glue job scripts that import pyspark, pyarrow and boto3 -- none of which a scheduler needs
    and DagBag would try to parse anything whose text mentions both "airflow" and "dag". Only
    dag-glue-workflow.py is ever deployed to a dags/ folder, so only it is what this parses.
    """
    here = os.path.dirname(os.path.abspath(__file__))
    bag = DagBag(dag_folder=os.path.join(here, DAG_FILE), include_examples=False)
    # import_errors is the assertion that matters most: a DAG whose module raises on import is
    # not a broken DAG, it is an ABSENT one -- the scheduler logs it and the UI shows nothing,
    # which looks identical to never having deployed the file.
    assert not bag.import_errors, f"DAG import errors: {bag.import_errors}"
    # bag.dags, not bag.get_dag(): get_dag() falls through to DagModel.get_current, which needs
    # an initialised Airflow metadata database. Parsing a file into a DAG object does not, and
    # a check that needs `airflow db init` first is one nobody runs.
    assert DAG_ID in bag.dags, f"{DAG_ID} not found in {sorted(bag.dags)}"
    return bag.dags[DAG_ID]


def test_dag_parses_and_graph_matches():
    dag = load_dag()

    assert set(dag.task_ids) == set(EXPECTED_EDGES), (
        f"task set drifted from the docstring: {sorted(dag.task_ids)}")
    for task_id, downstream in EXPECTED_EDGES.items():
        found = set(dag.get_task(task_id).downstream_task_ids)
        assert found == downstream, f"{task_id} -> {sorted(found)}, expected {sorted(downstream)}"

    # The fork, stated as a property rather than read off the picture: the three path tasks
    # are mutually independent. Chain them by accident and the DAG would assert a dependency
    # between sub-refineries that share step numbers and nothing else.
    paths = ['refine_path1', 'refine_path2', 'refine_path3']
    for task_id in paths:
        task = dag.get_task(task_id)
        assert not ({t for t in paths} & set(task.downstream_task_ids | task.upstream_task_ids)), \
            f"{task_id} is wired to another path task -- the three paths are independent"


def test_every_glue_task_waits_for_completion():
    """The bug in the reference lab, asserted away.

    Its poller exits on any non-running state and logs success, so a FAILED Glue job yields a
    green task and the next one runs against last month's artifacts. wait_for_completion=False
    here would reintroduce exactly that: the task would return the moment the job was submitted.
    """
    dag = load_dag()
    glue_tasks = [t for t in dag.tasks if isinstance(t, GlueJobOperator)]
    assert len(glue_tasks) == 5, f"expected 5 Glue jobs, found {len(glue_tasks)}"
    for task in glue_tasks:
        assert task.wait_for_completion is True, \
            f"{task.task_id} does not wait -- a failed Glue job would pass as success"


def test_the_month_can_actually_reach_the_jobs():
    """`script_args` must be a template field, or MONTH ships as literal Jinja.

    This is the one assumption in the DAG that is about the provider rather than about this
    repository, and it fails silently in the worst way: the job would be handed the string
    ``{{ data_interval_start.strftime('%Y-%m') }}`` as its ``--month``, and the entryway would
    build a bar calendar for a month of that name.
    """
    assert 'script_args' in GlueJobOperator.template_fields, (
        "GlueJobOperator.script_args is not templated in this provider version, so MONTH "
        "would be passed through literally -- pin apache-airflow-providers-amazon >= 8.0")

    dag = load_dag()
    # And the month must be present in every job that needs it: all three prefixes plus the
    # entryway's --month and the loader's --run-id.
    for task_id in ['ingest_bars', 'refine_path1', 'refine_path2', 'refine_path3', 'load_dynamo']:
        rendered = ' '.join(dag.get_task(task_id).script_args.values())
        assert 'data_interval_start' in rendered, \
            f"{task_id} carries no month -- its output would collide across runs"

    # --run-id and the path prefixes have to be the SAME month, or DynamoDB's run-scoped
    # history points at S3 artifacts written under a different key.
    loader = dag.get_task('load_dynamo').script_args
    assert loader['--run-id'] == dag.get_task('ingest_bars').script_args['--month'], \
        "the loader's --run-id and the entryway's --month are not the same expression"
    for path in ['path1', 'path2', 'path3']:
        assert loader[f'--{path}'] == dag.get_task(f'refine_{path}').script_args['--output'], \
            f"the loader reads a different prefix than refine_{path} writes"


def test_no_local_flag_reaches_glue():
    """--local sets a Spark master, which on Glue comes from the cluster."""
    dag = load_dag()
    for task in dag.tasks:
        if isinstance(task, GlueJobOperator):
            assert '--local' not in task.script_args, f"{task.task_id} passes --local to Glue"


if __name__ == '__main__':
    for name, fn in sorted(globals().items()):
        if name.startswith('test_') and callable(fn):
            fn()
            print(f"ok  {name}")
    print("all DAG checks passed")
