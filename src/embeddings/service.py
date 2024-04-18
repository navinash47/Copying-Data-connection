from langchain.embeddings import SentenceTransformerEmbeddings

embeddings_function = SentenceTransformerEmbeddings(model_name='intfloat/multilingual-e5-base',
                                                    encode_kwargs={'normalize_embeddings': True})
