#!/bin/bash

# === Конфигурация ===
GROUP_ID="com.example"
ARTIFACT_ID="demo-lib"
VERSIONS=("2.4" "2.5")
OUTPUT_DIR="./artifacts"

JAVA_CODE='package com.example;

public class HelloWorld {
    public static void main(String[] args) {
        System.out.println("Hello from demo-lib version: " + HelloWorld.class.getPackage().getImplementationVersion());
    }
    
    public static String getVersion() {
        return HelloWorld.class.getPackage().getImplementationVersion();
    }
}'

# Очистка и создание директории
rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"

for VERSION in "${VERSIONS[@]}"; do
  for TYPE in "release" "snapshot"; do
    if [ "$TYPE" = "snapshot" ]; then
      VERSION_FULL="${VERSION}-SNAPSHOT"
    else
      VERSION_FULL="${VERSION}"
    fi

    BUILD_DIR="./build-${VERSION_FULL}"
    rm -rf "${BUILD_DIR}"
    mkdir -p "${BUILD_DIR}/src/main/java/com/example"

    # Java файл
    echo "$JAVA_CODE" > "${BUILD_DIR}/src/main/java/com/example/HelloWorld.java"

    # POM файл
    cat > "${BUILD_DIR}/pom.xml" <<EOF
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 
                             http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>${GROUP_ID}</groupId>
  <artifactId>${ARTIFACT_ID}</artifactId>
  <version>${VERSION_FULL}</version>
  <packaging>jar</packaging>
  
  <properties>
    <maven.compiler.source>8</maven.compiler.source>
    <maven.compiler.target>8</maven.compiler.target>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
  </properties>
  
  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-jar-plugin</artifactId>
        <version>3.3.0</version>
        <configuration>
          <archive>
            <manifest>
              <addDefaultImplementationEntries>true</addDefaultImplementationEntries>
            </manifest>
          </archive>
        </configuration>
      </plugin>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-compiler-plugin</artifactId>
        <version>3.11.0</version>
        <configuration>
          <source>8</source>
          <target>8</target>
        </configuration>
      </plugin>
    </plugins>
  </build>
</project>
EOF

    # Сборка
    echo "🔨 Собираем: ${ARTIFACT_ID}-${VERSION_FULL}"
    (
      cd "$BUILD_DIR"
      mvn clean package -q
    )

    # Копируем артефакты
    cp "${BUILD_DIR}/target/${ARTIFACT_ID}-${VERSION_FULL}.jar" "${OUTPUT_DIR}/" 2>/dev/null || echo "⚠️  JAR не найден для ${VERSION_FULL}"
    cp "${BUILD_DIR}/pom.xml" "${OUTPUT_DIR}/${ARTIFACT_ID}-${VERSION_FULL}.pom"

    # Очистка
    rm -rf "$BUILD_DIR"
    echo "✅ Собрано: ${ARTIFACT_ID}-${VERSION_FULL}"
  done
done

echo ""
echo "📦 Все артефакты созданы в: $OUTPUT_DIR"
echo "Сгенерированные файлы:"
ls -la "$OUTPUT_DIR"/