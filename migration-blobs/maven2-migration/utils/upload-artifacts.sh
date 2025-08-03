#!/bin/bash

# === Конфигурация ===
NEXUS_URL="https://nexus.sanich.space"
REPO_ID="source-maven2"
GROUP_ID="com.example"
ARTIFACT_ID="demo-lib"
USERNAME="usr"
PASSWORD="pswrd"  # 🔐 Замени на реальный пароль
ARTIFACT_DIR="./artifacts"

# === Загрузка всех версий ===
for VERSION in 1.0 2.0; do
  echo "🚀 Загружаем версию: $VERSION"
  mvn deploy:deploy-file \
    -Durl=${NEXUS_URL}/repository/${REPO_ID}/ \
    -DrepositoryId=${REPO_ID} \
    -DgroupId=${GROUP_ID} \
    -DartifactId=${ARTIFACT_ID} \
    -Dversion=${VERSION} \
    -Dpackaging=jar \
    -Dfile=${ARTIFACT_DIR}/${ARTIFACT_ID}-${VERSION}.jar \
    -DpomFile=${ARTIFACT_DIR}/${ARTIFACT_ID}-${VERSION}.pom \
    -DgeneratePom=false \
    -DretryFailedDeploymentCount=3 \
    -DuniqueVersion=false \
    -Dusername=${USERNAME} \
    -Dpassword=${PASSWORD}
done
