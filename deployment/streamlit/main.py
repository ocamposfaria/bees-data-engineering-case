import streamlit as st
import pandas as pd
import duckdb
import sqlparse
import s3fs
import pyarrow.parquet as pq
from openai import OpenAI

# Título e subtítulo
st.title("Ourinho da ABInBev 🤖🍺")
st.markdown(
    "<p style='font-size:18px; color:gray;'>O mascote oficial da camada gold. Faço queries usando linguagem natural :)</p>",
    unsafe_allow_html=True
)

# Configurar cliente OpenAI
client = OpenAI(api_key="")

# Nome da tabela que será usada no DuckDB
tabela_nome = "breweries_by_type_location"

import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.fs

# Cria um filesystem para o MinIO usando pyarrow
minio_fs = pyarrow.fs.S3FileSystem(
    access_key='ROOTUSER',
    secret_key='CHANGEME123',
    endpoint_override='http://localhost:9000',
    region='us-east-1'
)

# Define o path do dataset
parquet_path = "datalake/3_gold/001_breweries/breweries_by_type_location"

# Lê o diretório Parquet inteiro como um dataset
dataset = ds.dataset(parquet_path, filesystem=minio_fs, format="parquet")

# Converte para pandas
df = dataset.to_table().to_pandas()

print(df)

# Extrai metadados da tabela
metadata = "\n".join([f"{col}: {dtype}" for col, dtype in zip(df.columns, df.dtypes)])

# Inicializar variáveis de sessão
if "openai_model" not in st.session_state:
    st.session_state["openai_model"] = "gpt-4"
if "messages" not in st.session_state:
    st.session_state.messages = []

# Mostrar histórico do chat
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if message["role"] == "assistant":
            if "query" in message:
                st.code(message["query"], language='sql')
            if "dataframe" in message:
                st.write("📊 Resultado da query:")
                st.dataframe(pd.DataFrame(message["dataframe"]))
            if "explanation" in message:
                st.write("🧠 Explicação do resultado:")
                st.write(message["explanation"])

# Input do usuário
if prompt := st.chat_input("Me pergunte sobre dados na camada gold!"):
    st.session_state.messages.append({"role": "user", "content": prompt})

    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        system_prompt = f"""
        Você é um assistente de dados com acesso à seguinte tabela chamada '{tabela_nome}'.

        Metadados da tabela:
        {metadata}

        Gere uma query SQL compatível com DuckDB que pode ser executada sobre esse dataframe.
        Se for possível gerar a query, retorne SOMENTE ELA.
        Se não for possível responder com os dados disponíveis, retorne "FALSE".
        SEMPRE LIMITE A CONSULTA EM 100 LINHAS.
        """

        messages = [{"role": "system", "content": system_prompt}]
        messages += [{"role": m["role"], "content": m["content"]} for m in st.session_state.messages]

        response = client.chat.completions.create(
            model=st.session_state["openai_model"],
            messages=messages,
            stream=False,
        )

        ai_reply = response.choices[0].message.content.strip()

        # Verificar se é uma query SQL
        if ai_reply != "FALSE" and ai_reply.lower().startswith("select"):
            try:
                con = duckdb.connect()
                con.register(tabela_nome, df)

                result_df = con.execute(ai_reply).fetchdf()
                formatted_query = sqlparse.format(ai_reply, reindent=True, keyword_case='upper')

                explanation_prompt = f"""
                O histórico do chat até agora foi: {st.session_state.messages}.
                A seguinte query foi executada sobre a tabela `{tabela_nome}`:
                {formatted_query}
                O resultado da query foi:
                {result_df.to_markdown(index=False)}
                Resuma o resultado obtido em uma frase."""

                explanation_response = client.chat.completions.create(
                    model=st.session_state["openai_model"],
                    messages=[
                        {"role": "system", "content": "Você explica resultados de consultas SQL de forma simples."},
                        {"role": "user", "content": explanation_prompt}
                    ]
                )
                explanation = explanation_response.choices[0].message.content

                # Mostrar na tela
                st.code(formatted_query, language='sql')
                st.write("📊 Resultado da query:")
                st.dataframe(result_df)
                st.write("🧠 Explicação do resultado:")
                st.write(explanation)

                # Salvar tudo na sessão
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": ai_reply,
                    "query": formatted_query,
                    "dataframe": result_df.to_dict(),  # Serializado
                    "explanation": explanation
                })

            except Exception as e:
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": f"Erro ao rodar a query: {e}"
                })
                st.error(f"Erro ao rodar a query: {e}")

        else:
            st.session_state.messages.append({
                "role": "assistant",
                "content": "Nenhuma query detectada."
            })
            st.info("""
                    Não foi possível fazer consulta. 
                    Lembre-se de apenas dou respostas caso consiga gerar 
                    uma query nos dados com base na sua pergunta! """)
