# Helm-чарт для установки приложения в Kubernetes

После успешной аутентификации в кластере склонируйте репозиторий этого чарта к себе на компьютер.

В файле `app/values.yaml` измените значения переменных. Укажите ссылку на реджистри, созданного в Yandex Cloud и версию образа:

```yaml
image:
  repository: "адрес образа в формате cr.yandex/<registry id>/<repo name>"
  pullPolicy: IfNotPresent
  # Overrides the image tag whose default is the chart appVersion.
  tag: "версия образа в реджистри"
```

Установите Helm-чарт:

```shell
helm upgrade --install --atomic test app
```

## Настройка реджистри Yandex Cloud

Для разрешения возможности пуллинга образов из вашего реджисти, настройте политику доступа. Нужно выдать роль `container-registry.images.puller` на ваш реестр для системной группы allUsers.

В настройках реджистри нажмите "Назначить роли" в правом верхнем углу и выберите группу "All Users":

<img src="img/regisry_all_users.png" alt="Contact Point" width="512"/>

Назначте этой группе роль `container-registry.images.puller`:

<img src="img/regisry_role.png" alt="Contact Point" width="512"/>


## Для изучения сообщений в Кафка:

export MSYS_NO_PATHCONV=1 docker run     -it     --name "kcat"     --network=host     --rm     -v 'C:/Users/алексей/.kafka/YandexInternalRootCA.crt:/data/CA.pem'     edenhill/kcat:1.7.1 -b rc1a-lrqhe7mbv67kq0gu.mdb.yandexcloud.net:9091     -X security.protocol=SASL_SSL     -X sasl.mechanisms=SCRAM-SHA-512     -X sasl.username=producer_consumer     -X sasl.password="****"     -X ssl.ca.location=/data/CA.pem     -t stg-service-orders     -C     -o beginning
