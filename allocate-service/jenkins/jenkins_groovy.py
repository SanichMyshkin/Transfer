import requests
import json


class JenkinsGroovyClient:
    def __init__(self, url, user=None, password=None, token=None, is_https=False):
        self.url = url.strip("/")
        self.is_https = is_https

        if token:
            self.auth = (user, token) if user else (token, "")
        else:
            self.auth = (user, password)

    def run_script(self, script, expect_json=True):
        endpoint = f"{self.url}/scriptText"
        try:
            resp = requests.post(
                endpoint,
                auth=self.auth,
                data={"script": script},
                verify=self.is_https,
                timeout=30,
            )
        except requests.RequestException as e:
            raise RuntimeError(f"Ошибка сети: {e}")

        if not resp.ok:
            raise RuntimeError(
                f"Jenkins вернул статус код {resp.status_code}: {resp.text}"
            )

        text = resp.text.strip()

        if text.startswith("<html") or text.startswith("<!DOCTYPE"):
            raise RuntimeError(
                "Jenkins вернул HTML страницу. Проверьте правильность auth-данных."
            )

        if text.startswith("Result: "):
            text = text[8:].strip()

        if expect_json:
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                if text.startswith("Result: "):
                    text = text[7:].strip()
                    try:
                        return json.loads(text)
                    except json.JSONDecodeError:
                        pass

                try:
                    text_fixed = text.strip("'").encode().decode("unicode_escape")
                    return json.loads(text_fixed)
                except Exception:
                    print("Преобразование JSON не вышло, возвращаем сырой текст")
                    return text
        else:
            return text
