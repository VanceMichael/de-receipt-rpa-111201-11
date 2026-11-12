import argparse
import json
from pathlib import Path
import sys
import tempfile

from .batch import BatchEngine, default_batch_id, load_applications, prepare_batch
from .batch_store import BatchStore
from .batch_types import BatchAlreadyRunning
from .domain import collect
from .locking import BatchLock
from .logging_setup import configure_logging
from .storage import Store
from .synthetic import FileBackedPortal
from .transport import Client, replay_server

EXIT_OK = 0
EXIT_USAGE = 2
EXIT_ATTENTION = 3
EXIT_LOCKED = 4


def _build_batch_parser(parser):
    parser.add_argument('--input', default='fixtures/portal.json', help='批次录入 JSON')
    parser.add_argument('--fixture', default='fixtures/portal.json', help='合成门户事件配置')
    parser.add_argument('--db', default='var/batch.sqlite3', help='批量状态数据库')
    parser.add_argument('--state', default='var/portal-state.json', help='合成门户持久化状态文件')
    parser.add_argument('--batch-id', default=None, help='覆盖按输入内容推导的批次标识')
    parser.add_argument('--recheck', action='store_true', help='只核对待确认条目，不发起新提交')
    parser.add_argument('--renew-session', action='store_true', help='人工重新登录后续跑（不自动刷新登录态）')


def _execute_batch(arguments):
    """执行一次批量处理，返回 (退出码, 报告或错误对象)。"""
    configure_logging()
    applications = load_applications(arguments.input)
    batch_id = arguments.batch_id or default_batch_id(applications)
    db_path = Path(arguments.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = db_path.with_suffix(f'.{batch_id}.lock')
    try:
        with BatchLock(lock_path):
            store = BatchStore(db_path)
            try:
                prepare_batch(store, batch_id, arguments.state, applications)
                fixture = json.loads(Path(arguments.fixture).read_bytes())
                portal = FileBackedPortal(arguments.state, fixture)
                try:
                    if arguments.renew_session:
                        portal.renew_session()
                    engine = BatchEngine(store, batch_id)
                    report = engine.run(portal, recheck_only=arguments.recheck)
                finally:
                    portal.close()
                report['journal'] = store.journal(batch_id)
            finally:
                store.close()
    except BatchAlreadyRunning as error:
        return EXIT_LOCKED, {'error': 'BATCH_ALREADY_RUNNING', 'message': str(error)}
    return (EXIT_OK if report['all_verified'] else EXIT_ATTENTION), report


def _run_batch(arguments):
    code, report = _execute_batch(arguments)
    if code == EXIT_LOCKED:
        print(json.dumps(report, ensure_ascii=False, indent=2), file=sys.stderr)
    else:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    return code


def _run_status(arguments):
    store = BatchStore(arguments.db)
    try:
        batches = store.list_batches()
        result = []
        for batch in batches:
            batch_id = batch['batch_id']
            if arguments.batch_id and batch_id != arguments.batch_id:
                continue
            entries = store.list_entries(batch_id)
            result.append({
                'batch': batch,
                'entries': [
                    {
                        'approval': row['approval'],
                        'state': row['state'],
                        'reason': row['reason'],
                        'receipt_number': row['receipt_number'],
                        'attempts': row['attempts'],
                        'updated_at': row['updated_at'],
                    }
                    for row in entries
                ],
                'journal': store.journal(batch_id),
            })
    finally:
        store.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return EXIT_OK


def _run_batch_demo(arguments):
    """端到端演示：正常批次、断线待核对、跨进程核实、会话过期与恢复。"""
    configure_logging()
    summary = {'passes': []}
    with tempfile.TemporaryDirectory() as directory:
        db = str(Path(directory) / 'batch.sqlite3')
        state = str(Path(directory) / 'portal-state.json')
        common = ['--input', arguments.fixture, '--fixture', arguments.fixture,
                  '--db', db, '--state', state]

        def invoke(extra, name):
            parsed = _parser().parse_args(['batch'] + common + extra)
            code, report = _execute_batch(parsed)
            status = {
                'name': name,
                'exit_code': code,
                'stopped': report.get('stopped'),
                'stop_reason': report.get('stop_reason'),
                'states': report.get('states'),
            }
            summary['passes'].append(status)
            return code

        # 第一遍：A100 提交后响应丢失（远端其实已受理并产生回执），
        # 同轮查询通道也不可用，保持待核对；A101 遇人工确认停止在草稿前。
        invoke([], '首次批量（断线待核对 + 人工确认停止）')

        # 第二遍：恢复时只查询不重提；查询通道仍不可用，继续保持待核对。
        invoke(['--recheck'], '恢复核对（查询不确定，保持待核对）')

        # 第三遍：会话已过期，批次停止在可解释的步骤，等待人工重新登录。
        invoke(['--recheck'], '恢复核对（登录过期，整批停止）')

        # 第四遍：人工重新登录后再核对——查询到 A100 真实回执，
        # “远端已办、本地未知”变为已核实；全程没有第二次提交。
        invoke(['--recheck', '--renew-session'], '重新登录后核对（待核对→已核实）')

        store = BatchStore(db)
        try:
            batch_id = default_batch_id(load_applications(arguments.fixture))
            entries = {row['approval']: row for row in store.list_entries(batch_id)}
            submit_attempts = sum(
                1 for item in store.journal(batch_id, 'A100')
                if item['event'] == 'submit_intent_persisted')
            summary['final_states'] = {key: row['state'] for key, row in entries.items()}
            summary['a100_submit_attempts'] = submit_attempts
            summary['a100_receipt'] = entries['A100']['receipt_number']
            summary['a101_block_reason'] = entries['A101']['reason']
            summary['journal'] = store.journal(batch_id)
        finally:
            store.close()
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return EXIT_OK


def _parser():
    parser = argparse.ArgumentParser(description='本地采集与申报门户可靠批量操作')
    sub = parser.add_subparsers(dest='command', required=True)

    p_demo = sub.add_parser('demo', help='原单次采集演示')
    p_demo.set_defaults(handler=_run_legacy_demo)
    p_demo.add_argument('--input', default='fixtures/sample.json')

    p_import = sub.add_parser('import', help='单次采集入库')
    p_import.set_defaults(handler=_run_legacy_import)
    p_import.add_argument('--input', default='fixtures/sample.json')
    p_import.add_argument('--db', default='var/records.sqlite3')

    p_show = sub.add_parser('show', help='查看采集记录')
    p_show.set_defaults(handler=_run_legacy_show)
    p_show.add_argument('--db', default='var/records.sqlite3')

    p_serve = sub.add_parser('serve', help='只读记录查询服务')
    p_serve.set_defaults(handler=_run_legacy_serve)
    p_serve.add_argument('--db', default='var/records.sqlite3')
    p_serve.add_argument('--port', type=int, default=8080)

    p_batch = sub.add_parser('batch', help='可靠批量自动操作（一条命令走完正常批次）')
    _build_batch_parser(p_batch)
    p_batch.set_defaults(handler=lambda args: _run_batch(args))

    p_status = sub.add_parser('status', help='查看批次状态与审计日志')
    p_status.add_argument('--db', default='var/batch.sqlite3')
    p_status.add_argument('--batch-id', default=None)
    p_status.set_defaults(handler=_run_status)

    p_bdemo = sub.add_parser('batch-demo', help='端到端演示不确定提交的恢复与核实')
    p_bdemo.add_argument('--fixture', default='fixtures/portal.json')
    p_bdemo.set_defaults(handler=_run_batch_demo)
    return parser


def _run_legacy_demo(arguments):
    with tempfile.TemporaryDirectory() as directory:
        payload = Path(arguments.input).read_bytes()
        with replay_server({'/source': [dict(body=payload.decode('utf-8'))]}) as (base, calls):
            status, headers, raw = Client().request(base + '/source')
            if status != 200:
                raise RuntimeError('本地来源未返回成功响应')
            store = Store(Path(directory) / 'demo.sqlite3')
            rows = collect(json.loads(raw))
            digest = store.ingest('本地演示来源', raw, rows)
            print(json.dumps(dict(digest=digest, records=store.records(), requests=len(calls)),
                             ensure_ascii=False, indent=2))
            store.close()
    return EXIT_OK


def _run_legacy_import(arguments):
    Path(arguments.db).parent.mkdir(parents=True, exist_ok=True)
    store = Store(arguments.db)
    try:
        raw = Path(arguments.input).read_bytes()
        store.ingest(str(Path(arguments.input)), raw, collect(json.loads(raw)))
        print(json.dumps(store.records(), ensure_ascii=False, indent=2))
    finally:
        store.close()
    return EXIT_OK


def _run_legacy_show(arguments):
    store = Store(arguments.db)
    try:
        print(json.dumps(store.records(), ensure_ascii=False, indent=2))
    finally:
        store.close()
    return EXIT_OK


def _run_legacy_serve(arguments):
    from .api import serve
    serve(arguments.db, arguments.port)
    return EXIT_OK


def main():
    arguments = _parser().parse_args()
    sys.exit(arguments.handler(arguments))


if __name__ == '__main__':
    main()
