import os
import json
import duckdb
import sqlparse
import pandas as pd
import streamlit as st
import pyarrow.dataset as ds
import pyarrow.fs
from openai import OpenAI


def load_metadata(file_path):
    with open(file_path, "r") as f:
        return json.load(f)


def format_metadata(metadata_dict):
    return "\n".join([f"{col}: {dtype}" for col, dtype in metadata_dict.items()])


def init_openai_client():
    return OpenAI(api_key=os.getenv("OPEN_AI_KEY"))


def display_title_and_intro():
    st.title("Ourinho da ABInBev 🤖🍺")
    st.markdown(
        "<p style='font-size:18px; color:gray;'>O mascote oficial da camada gold. Faço queries usando linguagem natural :)</p>",
        unsafe_allow_html=True
    )


def initialize_session_state():
    if "openai_model" not in st.session_state:
        st.session_state["openai_model"] = "gpt-4"
    if "messages" not in st.session_state:
        st.session_state.messages = []


def render_previous_messages():
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            if "query" not in message:
                st.markdown(message["content"])

            if message["role"] == "assistant":
                if "query" in message:
                    st.write("👨‍💻 Query:")
                    st.code(sqlparse.format(message["query"], reindent=True, keyword_case='upper'), language='sql')
                if "dataframe" in message:
                    st.write("📊 Resultado da query:")
                    st.dataframe(pd.DataFrame(message["dataframe"]))
                if "explanation" in message:
                    st.write("🧠 Explicação do resultado:")
                    st.write(message["explanation"])
                if message['content'] == "Nenhuma query detectada.":
                    st.write("""
                        Não foi possível fazer consulta. 
                        Lembre-se de que só dou respostas caso consiga gerar 
                        uma query nos dados com base na sua pergunta! """)


def generate_sql_query(prompt, metadata, client, model, history, tabela_nome):
    system_prompt = f"""
    Você é um assistente de dados com acesso à seguinte tabela chamada '{tabela_nome}'.

    Metadados da tabela:
    {metadata}

    - Gere uma query SQL compatível com DuckDB que pode ser executada sobre esse dataframe.
    Se for possível gerar a query, retorne SOMENTE ELA.
    - Se não for possível responder com os dados disponíveis, retorne "FALSE".
    SEMPRE LIMITE A CONSULTA EM 100 LINHAS.
    - Se o usuário perguntar algo genérico, ou perguntar sobre quais os dados disponíveis, gere uma query genéria (SELECT * por exemplo).
    E se ele perguntar quais tabelas estão disponíveis, gere uma query com o nome da tabela.
    """

    messages = [{"role": "system", "content": system_prompt}]
    messages += [{"role": m["role"], "content": m["content"]} for m in history]

    response = client.chat.completions.create(
        model=model,
        messages=messages,
        stream=False,
    )
    return response.choices[0].message.content.strip()


def get_explanation(client, model, history, tabela_nome, formatted_query, result_df):
    explanation_prompt = f"""
    O histórico do chat até agora foi: {history}.
    A seguinte query foi executada sobre a tabela `{tabela_nome}`:
    {formatted_query}
    O resultado da query foi:
    {result_df.to_markdown(index=False)}
    Resuma o resultado obtido em uma frase."""

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "Você explica resultados de consultas SQL de forma simples."},
            {"role": "user", "content": explanation_prompt}
        ]
    )
    return response.choices[0].message.content


def execute_query(query, tabela_nome):
    minio_fs = pyarrow.fs.S3FileSystem(
        access_key=os.getenv("MINIO_ROOT_USER"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
        endpoint_override='http://minio:9000',
        region='us-east-1'
    )
    parquet_path = "datalake/3_gold/001_breweries/breweries_by_type_location"
    dataset = ds.dataset(parquet_path, filesystem=minio_fs, format="parquet")
    df = dataset.to_table().to_pandas()
    con = duckdb.connect()
    con.register(tabela_nome, df)
    result_df = con.execute(query).fetchdf()
    return result_df


def main():
    tabela_nome = "breweries_by_type_location"
    metadata_dict = load_metadata("gold_metadata/gold_001_breweries.json")
    metadata_str = format_metadata(metadata_dict)

    client = init_openai_client()
    display_title_and_intro()
    initialize_session_state()
    render_previous_messages()

    if prompt := st.chat_input("Me pergunte sobre dados na camada gold! tabela(s): breweries_by_type_location"):
        st.session_state.messages.append({"role": "user", "content": prompt})

        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            ai_reply = generate_sql_query(
                prompt=prompt,
                metadata=metadata_str,
                client=client,
                model=st.session_state["openai_model"],
                history=st.session_state.messages,
                tabela_nome=tabela_nome
            )

            if ai_reply != "FALSE" and ai_reply.lower().startswith("select"):
                try:
                    result_df = execute_query(ai_reply, tabela_nome)
                    formatted_query = sqlparse.format(ai_reply, reindent=True, keyword_case='upper')
                    explanation = get_explanation(
                        client=client,
                        model=st.session_state["openai_model"],
                        history=st.session_state.messages,
                        tabela_nome=tabela_nome,
                        formatted_query=formatted_query,
                        result_df=result_df
                    )

                    st.write("👨‍💻 Query:")
                    st.code(formatted_query, language='sql')
                    st.write("📊 Resultado da query:")
                    st.dataframe(result_df)
                    st.write("🧠 Explicação do resultado:")
                    st.write(explanation)

                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": ai_reply,
                        "query": formatted_query,
                        "dataframe": result_df.to_dict(),
                        "explanation": explanation
                    })

                except Exception as e:
                    error_message = f"Erro ao rodar a query: {e}"
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": error_message
                    })
                    st.error(error_message)

            else:
                info_message = """
                    Não foi possível fazer consulta. 
                    Lembre-se de que só dou respostas caso consiga gerar 
                    uma query nos dados com base na sua pergunta! 
                    """
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": info_message
                })
                st.write(info_message)


if __name__ == "__main__":
    main()
