# Databricks notebook source
# MAGIC %md
# MAGIC Referencia:
# MAGIC
# MAGIC Conectar-se ao Azure Data Lake Storage Gen2 e ao Armazenamento de Blobs:
# MAGIC https://learn.microsoft.com/pt-br/azure/databricks/storage/azure-storage
# MAGIC
# MAGIC Montar o ADLS Gen2 ou o Armazenamento de Blobs com o ABFS:
# MAGIC https://learn.microsoft.com/pt-br/azure/databricks/dbfs/mounts
# MAGIC
# MAGIC
# MAGIC Obs diferença de DBFS E ABFS:
# MAGIC
# MAGIC DBFS (Databricks File System): treinamos em Community
# MAGIC
# MAGIC ABFS (Azure Blob Storage File System) : Arquivos em Nuvem

# COMMAND ----------


#Config Padrão

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "<application-id>",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="<scope-name>",key="<service-credential-key-name>"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory-id>/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/",
  mount_point = "/mnt/<mount-name>",
  extra_configs = configs)

#Substitua

#<application-id> com a ID de aplicativo (cliente) para o aplicativo do Azure Active Directory.
#<scope-name> pelo nome do escopo de segredo do Databricks.
#<service-credential-key-name> com o nome da chave que contém o segredo do cliente.
#<directory-id> com a ID do Diretório (locatário) ID para o aplicativo do Azure Active Directory.
#<container-name> pelo nome de um contêiner na conta de armazenamento do ADLS Gen2.
#<storage-account-name> pelo nome da conta de armazenamento do ADLS Gen2.
#<mount-name> com o nome do ponto de montagem pretendido no DBFS.

# COMMAND ----------

# MAGIC %md
# MAGIC Montando Armazenamento 
# MAGIC

# COMMAND ----------

# criar uma "pasta no Dantabricks para fazer vinculo"

# COMMAND ----------

# MAGIC %fs mkdirs /mnt/SuaPasta
