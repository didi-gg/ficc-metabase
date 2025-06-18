# Imagen base compatible con ARM64
FROM openjdk:19-buster

# Versión de Metabase a usar
ENV METABASE_VERSION=v0.49.12

# Descargar el archivo JAR de Metabase
ADD https://downloads.metabase.com/${METABASE_VERSION}/metabase.jar /home/metabase.jar

# Establecer el directorio de trabajo
WORKDIR /home

# Puerto expuesto
EXPOSE 3000

# Comando para ejecutar Metabase
CMD ["java", "-jar", "/home/metabase.jar"]