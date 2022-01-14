package com.faster.kafkatester;

import android.annotation.SuppressLint;
import android.os.Bundle;
import android.os.StrictMode;
import android.util.Log;
import android.view.View;
import android.widget.AdapterView;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Spinner;
import android.widget.Toast;

import androidx.appcompat.app.AppCompatActivity;

import com.android.volley.AuthFailureError;
import com.android.volley.NetworkResponse;
import com.android.volley.Request;
import com.android.volley.RequestQueue;
import com.android.volley.Response;
import com.android.volley.ServerError;
import com.android.volley.VolleyError;
import com.android.volley.toolbox.HttpHeaderParser;
import com.android.volley.toolbox.JsonArrayRequest;
import com.android.volley.toolbox.StringRequest;
import com.android.volley.toolbox.Volley;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

public class MainActivity extends AppCompatActivity implements View.OnClickListener, AdapterView.OnItemSelectedListener {

    private static final HostnameVerifier DO_NOT_VERIFY = new HostnameVerifier() {
        @SuppressLint("BadHostnameVerifier")
        public boolean verify(String hostname, SSLSession session) {
            return true;
        }
    };

    private Button btnPost;
    private Button btnGet;
    private Button btnSet;
    private Button btnAttach;

    private Spinner spinnerKafka;
    private ArrayAdapter<CharSequence> kafka_list;

    private EditText editTopic;
    private EditText editSize;

    private String topic;
    private String token;
    private String selectedKafka;
    private String kafkaURL;
    private String tokenURL;
    private String message;
    private String consumerName;
    private String consumerType;

    private int kafkaPos;
    private int size;

    private boolean isSecure;

    private RequestQueue postQueue, getQueue, consumerQueue;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        StrictMode.ThreadPolicy policy = new StrictMode.ThreadPolicy.Builder().permitAll().build();
        StrictMode.setThreadPolicy(policy);
        main();
    }

    public void main() {
        token = "";
        tokenURL = "";
        kafkaURL = "";
        isSecure = false;
        consumerName = "testConsumer";
        consumerType = "latest";

        btnPost = findViewById(R.id.btn_post);
        btnGet = findViewById(R.id.btn_get);
        btnSet = findViewById(R.id.btn_set);
        btnAttach = findViewById(R.id.btn_attach);

        spinnerKafka = findViewById(R.id.spinner_kafka);
        kafka_list = ArrayAdapter.createFromResource(this, R.array.kafka_brokers, android.R.layout.simple_spinner_item);
        kafka_list.setDropDownViewResource(android.R.layout.simple_spinner_dropdown_item);
        spinnerKafka.setAdapter(kafka_list);

        editTopic = findViewById(R.id.editTopic);
        editSize = findViewById(R.id.editSize);

        btnPost.setOnClickListener(this);
        btnGet.setOnClickListener(this);
        btnSet.setOnClickListener(this);
        btnAttach.setOnClickListener(this);
        spinnerKafka.setOnItemSelectedListener(this);

        postQueue = Volley.newRequestQueue(this);
        getQueue = Volley.newRequestQueue(this);
        consumerQueue = Volley.newRequestQueue(this);

        generateConfigFile();

        readConfigFile();

        message = generateRandomMessage(size);
        //message = "aMessage";
    }

    @SuppressLint("NonConstantResourceId")
    @Override
    public void onClick(View v) {
        writeConfigFile();
        switch (v.getId()) {
            case R.id.btn_post:

                makePOST(kafkaURL, message, topic);
                break;

            case R.id.btn_get:
                makeGET(kafkaURL, consumerName);
                break;

            case R.id.btn_set:
                getURLs();
                showLongToast("Kafka: " + selectedKafka + "\nSecure: " + isSecure + "\nTopic: " + topic + "\nSize: " + size);
                break;

            case R.id.btn_attach:
                makeConsumer(kafkaURL, consumerName, consumerType);
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                makeSubscription(kafkaURL, consumerName, topic);
                break;

            default:
                break;
        }
    }

    @Override
    public void onItemSelected(AdapterView<?> adapterView, View view, int i, long l) {
        kafkaPos = i;
        selectedKafka = adapterView.getItemAtPosition(i).toString();
    }

    @Override
    public void onNothingSelected(AdapterView<?> adapterView) {

    }

    private void generateConfigFile() {
        String path = getFilesDir() + "/config.json";

        File settingsFile = new File(path);

        if (!settingsFile.exists()) {
            JSONObject settingsJSON = new JSONObject();
            try {
                settingsJSON.put("kafka", "0");
                settingsJSON.put("topic", "test");
                settingsJSON.put("size", "1");

                FileWriter writer = new FileWriter(path);
                writer.write(settingsJSON.toString());
                writer.flush();

            } catch (JSONException | IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void readConfigFile() {
        String path = getFilesDir() + "/config.json";

        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
            StringBuilder stringBuilder = new StringBuilder();
            String line = bufferedReader.readLine();
            while (line != null) {
                stringBuilder.append(line).append("\n");
                line = bufferedReader.readLine();
            }
            bufferedReader.close();

            String contents = stringBuilder.toString();
            JSONObject configJSON = new JSONObject(contents);

            kafkaPos = Integer.parseInt(configJSON.getString("kafka"));
            topic = configJSON.getString("topic");
            size = Integer.parseInt(configJSON.getString("size"));

            editTopic.setText(topic);
            editSize.setText(String.valueOf(size));
            spinnerKafka.setSelection(kafkaPos);

        } catch (IOException | JSONException e) {
            e.printStackTrace();
        }
    }

    private void writeConfigFile() {
        String path = getFilesDir() + "/config.json";

        topic = editTopic.getText().toString();
        size = Integer.parseInt(editSize.getText().toString());

        JSONObject configJSON = new JSONObject();

        try {
            configJSON.put("kafka", String.valueOf(spinnerKafka.getSelectedItemPosition()));
            configJSON.put("topic", topic);
            configJSON.put("size", String.valueOf(size));

            FileWriter writer = new FileWriter(path);
            writer.write(configJSON.toString());
            writer.flush();
        } catch (JSONException | IOException e) {
            e.printStackTrace();
        }
    }

    private void getURLs() {
        if (kafkaPos == 0) {
            kafkaURL = "http://faster.inov.pt:8082";
            isSecure = false;
            tokenURL = "";
            token = "";
        } else if (kafkaPos == 1) {
            kafkaURL = "http://faster2.inov.pt:8444";
            isSecure = true;
            tokenURL = "https://faster2.inov.pt:8443/auth/realms/faster/protocol/openid-connect/token";
            token = getAccessToken(tokenURL);
        } else if (kafkaPos == 2) {
            kafkaURL = "https://faster-fog.ecosys.eu:8444/ikafkarest";
            isSecure = true;
            tokenURL = "https://faster-fog.ecosys.eu:8443/auth/realms/faster/protocol/openid-connect/token";
            token = getAccessToken(tokenURL);
        }

    }

    private void makePOST(String url, String message, String topic) {
        String URL = url + "/topics/" + topic;
        StringRequest stringRequest;

        if (isSecure) {
            stringRequest = new StringRequest(Request.Method.POST, URL, new Response.Listener<String>() {
                @Override
                public void onResponse(String response) {
                    Log.i("makePOST", "URL: " + URL + "\nonResponse: " + response);
                }
            }, new Response.ErrorListener() {
                @Override
                public void onErrorResponse(VolleyError error) {
                    NetworkResponse response = error.networkResponse;
                    if (error instanceof ServerError && response != null) {
                        try {
                            String res = new String(response.data, HttpHeaderParser.parseCharset(response.headers, "utf-8"));
                            Log.w("makePOST", "URL: " + URL + "\nonErrorResponse: " + error.getMessage() + "\nResponse: " + res);
                        } catch (UnsupportedEncodingException e1) {
                            e1.printStackTrace();
                        }
                    }
                }
            }) {
                @Override
                public Map<String, String> getHeaders() throws AuthFailureError {
                    HashMap<String, String> headers = new HashMap<>();
                    headers.put("Authorization", "Bearer " + token);
                    return headers;
                }

                @Override
                public String getBodyContentType() {
                    return "application/vnd.kafka.json.v2+json";
                }

                @Override
                public byte[] getBody() {
                    return message == null ? null : message.getBytes();
                }
            };

        } else {
            stringRequest = new StringRequest(Request.Method.POST, URL, new Response.Listener<String>() {
                @Override
                public void onResponse(String response) {
                    Log.i("makePOST", "URL: " + URL + "\nonResponse: " + response);
                }
            }, new Response.ErrorListener() {
                @Override
                public void onErrorResponse(VolleyError error) {
                    NetworkResponse response = error.networkResponse;
                    if (error instanceof ServerError && response != null) {
                        try {
                            String res = new String(response.data, HttpHeaderParser.parseCharset(response.headers, "utf-8"));
                            Log.w("makePOST", "URL: " + URL + "\nonErrorResponse: " + error.getMessage() + "\nResponse: " + res);
                        } catch (UnsupportedEncodingException e1) {
                            e1.printStackTrace();
                        }
                    }
                }
            }) {
                @Override
                public String getBodyContentType() {
                    return "application/vnd.kafka.json.v2+json";
                }

                @Override
                public byte[] getBody() {
                    return message == null ? null : message.getBytes();
                }
            };
        }

        postQueue.add(stringRequest);
    }

    private void makeGET(String url, String consumerName) {
        String URL = url + "/consumers/" + consumerName + "/instances/" + consumerName + "/records";

        JsonArrayRequest jsonArrayRequest = new JsonArrayRequest(Request.Method.GET, URL, null, new Response.Listener<JSONArray>() {
            @Override
            public void onResponse(JSONArray response) {
                Log.i("makeGET", "URL: " + URL + "\nonResponse: " + response);
            }
        }, new Response.ErrorListener() {
            @Override
            public void onErrorResponse(VolleyError error) {
                NetworkResponse response = error.networkResponse;
                if (error instanceof ServerError && response != null) {
                    try {
                        String res = new String(response.data, HttpHeaderParser.parseCharset(response.headers, "utf-8"));
                        Log.w("makeGet", "URL: " + URL + "\nonErrorResponse: " + error.getMessage() + "\nResponse: " + res);
                    } catch (UnsupportedEncodingException e1) {
                        e1.printStackTrace();
                    }
                }
            }
        }) {
            @Override
            public Map<String, String> getHeaders() {
                HashMap<String, String> headers = new HashMap<>();
                headers.put("Accept", "application/vnd.kafka.json.v2+json");
                if (isSecure) {
                    headers.put("Authorization", "Bearer " + token);
                }
                return headers;
            }
        };
        getQueue.add(jsonArrayRequest);
    }

    private void makeConsumer(String url, String consumerName, String autoOffsetReset) {
        String URL = url + "/consumers/" + consumerName;
        String data = "{\"name\": \"" + consumerName + "\", \"format\": \"json\", \"auto.offset.reset\": \"" + autoOffsetReset + "\", \"consumer.request.timeout.ms\": 10}";
        StringRequest stringRequest;

        if (isSecure) {
            stringRequest = new StringRequest(Request.Method.POST, URL, new Response.Listener<String>() {
                @Override
                public void onResponse(String response) {
                    Log.i("makeConsumer", "onResponse: " + response);
                }
            }, new Response.ErrorListener() {
                @Override
                public void onErrorResponse(VolleyError error) {
                    NetworkResponse response = error.networkResponse;
                    if (error instanceof ServerError && response != null) {
                        try {
                            String res = new String(response.data, HttpHeaderParser.parseCharset(response.headers, "utf-8"));
                            Log.w("makeConsumer", "URL: " + URL + "\nonErrorResponse: " + error.getMessage() + "\nResponse: " + res);
                        } catch (UnsupportedEncodingException e1) {
                            e1.printStackTrace();
                        }
                    }
                }
            }) {
                @Override
                public Map<String, String> getHeaders() throws AuthFailureError {
                    HashMap<String, String> headers = new HashMap<>();
                    headers.put("Authorization", "Bearer " + token);
                    return headers;
                }

                @Override
                public String getBodyContentType() {
                    return "application/vnd.kafka.json.v2+json";
                }

                @Override
                public byte[] getBody() {
                    return data.getBytes();
                }
            };

        } else {
            stringRequest = new StringRequest(Request.Method.POST, URL, new Response.Listener<String>() {
                @Override
                public void onResponse(String response) {
                    Log.i("makeConsumer", "onResponse: " + response);
                }
            }, new Response.ErrorListener() {
                @Override
                public void onErrorResponse(VolleyError error) {
                    NetworkResponse response = error.networkResponse;
                    if (error instanceof ServerError && response != null) {
                        try {
                            String res = new String(response.data, HttpHeaderParser.parseCharset(response.headers, "utf-8"));
                            Log.w("makeConsumer", "URL: " + URL + "\nonErrorResponse: " + error.getMessage() + "\nResponse: " + res);
                        } catch (UnsupportedEncodingException e1) {
                            e1.printStackTrace();
                        }
                    }
                }
            }) {

                @Override
                public String getBodyContentType() {
                    return "application/vnd.kafka.json.v2+json";
                }

                @Override
                public byte[] getBody() {
                    return data.getBytes();
                }
            };
        }

        consumerQueue.add(stringRequest);
    }

    private void makeSubscription(String url, String consumerName, String topic) {
        String URL = url + "/consumers/" + consumerName + "/instances/" + consumerName + "/subscription";
        String data = "{\"topics\":[\"" + topic + "\"]}";
        StringRequest stringRequest;

        if (isSecure) {
            stringRequest = new StringRequest(Request.Method.POST, URL, new Response.Listener<String>() {
                @Override
                public void onResponse(String response) {
                    Log.i("makeSubscription", "onResponse: " + response);
                }
            }, new Response.ErrorListener() {
                @Override
                public void onErrorResponse(VolleyError error) {
                    NetworkResponse response = error.networkResponse;
                    if (error instanceof ServerError && response != null) {
                        try {
                            String res = new String(response.data, HttpHeaderParser.parseCharset(response.headers, "utf-8"));
                            Log.w("makeSubscription", "URL: " + URL + "\nonErrorResponse: " + error.getMessage() + "\nResponse: " + res);
                        } catch (UnsupportedEncodingException e1) {
                            e1.printStackTrace();
                        }
                    }
                }
            }) {
                @Override
                public Map<String, String> getHeaders() throws AuthFailureError {
                    HashMap<String, String> headers = new HashMap<>();
                    headers.put("Authorization", "Bearer " + token);
                    return headers;
                }

                @Override
                public String getBodyContentType() {
                    return "application/vnd.kafka.json.v2+json";
                }

                @Override
                public byte[] getBody() {
                    return data.getBytes();
                }
            };

        } else {
            stringRequest = new StringRequest(Request.Method.POST, URL, new Response.Listener<String>() {
                @Override
                public void onResponse(String response) {
                    Log.i("makeSubscription", "onResponse: " + response);
                }
            }, new Response.ErrorListener() {
                @Override
                public void onErrorResponse(VolleyError error) {
                    NetworkResponse response = error.networkResponse;
                    if (error instanceof ServerError && response != null) {
                        try {
                            String res = new String(response.data, HttpHeaderParser.parseCharset(response.headers, "utf-8"));
                            Log.w("makeSubscription", "URL: " + URL + "\nonErrorResponse: " + error.getMessage() + "\nResponse: " + res);
                        } catch (UnsupportedEncodingException e1) {
                            e1.printStackTrace();
                        }
                    }
                }
            }) {
                @Override
                public String getBodyContentType() {
                    return "application/vnd.kafka.json.v2+json";
                }

                @Override
                public byte[] getBody() {
                    return data.getBytes();
                }
            };
        }

        consumerQueue.add(stringRequest);
    }

    private String getAccessToken(String ip) {
        HttpURLConnection connection;
        String token = "";
        URL url;

        @SuppressLint("CustomX509TrustManager")
        TrustManager[] trustAllCertificates = new TrustManager[]{new X509TrustManager() {

            @SuppressLint("TrustAllX509TrustManager")
            @Override
            public void checkClientTrusted(java.security.cert.X509Certificate[] x509Certificates, String s) {

            }

            @SuppressLint("TrustAllX509TrustManager")
            @Override
            public void checkServerTrusted(java.security.cert.X509Certificate[] x509Certificates, String s) {

            }

            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                return new java.security.cert.X509Certificate[]{};
            }

        }};

        try {
            url = new URL(ip);

            if ("https".equalsIgnoreCase(url.getProtocol())) {
                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(null, trustAllCertificates, new java.security.SecureRandom());

                HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
                HttpsURLConnection secureConnection = (HttpsURLConnection) url.openConnection();
                secureConnection.setHostnameVerifier(DO_NOT_VERIFY);

                connection = secureConnection;
                connection.setConnectTimeout(3000); //milliseconds
                connection.setRequestMethod("POST");
                connection.setDoOutput(true);
                connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

                String data = "grant_type=client_credentials&client_id=faster-gs&client_secret=f9a2d780-eb0f-4bd7-b68f-e1d008a1bd33";

                OutputStream stream = connection.getOutputStream();
                stream.write(data.getBytes(StandardCharsets.UTF_8));

                int code = connection.getResponseCode();

                if (code == 200) {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                    StringBuilder sb = new StringBuilder();

                    while (true) {
                        String line;
                        if ((line = reader.readLine()) == null) break;
                        sb.append(line).append("\n");
                    }

                    reader.close();

                    String jsonString = sb.toString();
                    JSONObject response = new JSONObject(jsonString);
                    token = response.getString("access_token");
                    showLongToast(token);

                } else {
                    showLongToast("Failed to acquire access token");
                }

                connection.disconnect();
            }
        } catch (IOException | JSONException | NoSuchAlgorithmException | KeyManagementException e) {
            e.printStackTrace();
        }
        return token;
    }

    private void showLongToast(String toastMsg) {
        runOnUiThread(new Runnable() {
            @Override
            public void run() {
                Toast.makeText(getApplicationContext(), toastMsg, Toast.LENGTH_LONG).show();
            }
        });
    }

    private void showShortToast(String toastMsg) {
        runOnUiThread(new Runnable() {
            @Override
            public void run() {
                Toast.makeText(getApplicationContext(), toastMsg, Toast.LENGTH_SHORT).show();
            }
        });
    }

    public String generateRandomMessage(int number) {
        int size = ((number * 1000) - 2);
        String AlphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + "abcdefghijklmnopqrstuvxyz" + "0123456789";
        StringBuilder sb = new StringBuilder(size);

        for (int i = 0; i < size; i++) {
            int index = (int) (AlphaNumericString.length() * Math.random());

            sb.append(AlphaNumericString.charAt(index));
        }

        return sb.toString();
    }
}