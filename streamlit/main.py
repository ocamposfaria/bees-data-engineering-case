import streamlit as st
import pandas as pd
import duckdb
import sqlparse
import json
from openai import OpenAI
import pyarrow.dataset as ds
import pyarrow.fs

# Título e subtítulo
st.title("Ourinho da ABInBev 🤖🍺")
st.markdown(
    "<p style='font-size:18px; color:gray;'>O mascote oficial da camada gold. Faço queries usando linguagem natural :)</p>",
    unsafe_allow_html=True
)

# Configurar cliente OpenAI (ideal passar a chave via secrets ou env var)
client = OpenAI(api_key="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")

# Nome da tabela que será usada no DuckDB
tabela_nome = "breweries_by_type_location"

# Lê os metadados do arquivo JSON
with open("gold_metadata/gold_001_breweries.json", "r") as f:
    gold_metadata = json.load(f)

# Formata os metadados para exibição no system prompt
metadata = "\n".join([f"{col}: {dtype}" for col, dtype in gold_metadata.items()])

# Inicializar variáveis de sessão
if "openai_model" not in st.session_state:
    st.session_state["openai_model"] = "gpt-4"
if "messages" not in st.session_state:
    st.session_state.messages = []

# Mostrar histórico do chat
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        # Só exibe o conteúdo bruto se não for uma query formatada
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
                info_message = """
                    Não foi possível fazer consulta. 
                    Lembre-se de que só dou respostas caso consiga gerar 
                    uma query nos dados com base na sua pergunta! """
                st.write(info_message)

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
                # Carrega o dataset APENAS quando a query for válida
                minio_fs = pyarrow.fs.S3FileSystem(
                    access_key='ROOTUSER',
                    secret_key='CHANGEME123',
                    endpoint_override='http://minio:9000',
                    region='us-east-1'
                )

                parquet_path = "datalake/3_gold/001_breweries/breweries_by_type_location"
                dataset = ds.dataset(parquet_path, filesystem=minio_fs, format="parquet")
                df = dataset.to_table().to_pandas()

                # Executa a query
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
                st.write("👨‍💻 Query:")
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
                # Mostrar erro corretamente
                error_message = f"Erro ao rodar a query: {e}"
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": error_message
                })
                st.error(error_message)

        else:
            # Mostrar .info corretamente no histórico
            info_message = """
                Não foi possível fazer consulta. 
                Lembre-se de que só dou respostas caso consiga gerar 
                uma query nos dados com base na sua pergunta! """
            st.session_state.messages.append({
                "role": "assistant",
                "content": info_message
            })
            st.write(info_message)

# para debug
# print(st.session_state.messages)