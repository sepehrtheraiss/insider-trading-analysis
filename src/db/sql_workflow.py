
from .sql_agent import SqlAgent, SqlAgentConfig
from .sql_runner import explain_analyze, run_query


def answer_question_with_sql(question: str):
    agent = SqlAgent(SqlAgentConfig())

    sql = agent.generate(question)

    semantic = agent.validate_semantics(question=question, sql=sql)
    if not semantic["is_correct"]:
        sql = semantic["corrected_sql"]

    plan = explain_analyze(sql)

    optimized = agent.optimize_from_explain(sql=sql, explain_text=plan)
    final_sql = optimized["improved_sql"]

    rows = run_query(final_sql)

    return {
        "question": question,
        "sql": sql,
        "optimized": optimized,
        "rows": rows,
    }
