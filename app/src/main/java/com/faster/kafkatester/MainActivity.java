package com.faster.kafkatester;

import android.annotation.SuppressLint;
import android.os.Bundle;
import android.os.StrictMode;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.CheckBox;
import android.widget.EditText;
import android.widget.Toast;

import androidx.appcompat.app.AppCompatActivity;

import com.android.volley.AuthFailureError;
import com.android.volley.Request;
import com.android.volley.RequestQueue;
import com.android.volley.Response;
import com.android.volley.VolleyError;
import com.android.volley.toolbox.StringRequest;
import com.android.volley.toolbox.Volley;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
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

public class MainActivity extends AppCompatActivity implements View.OnClickListener {

    private static final HostnameVerifier DO_NOT_VERIFY = new HostnameVerifier() {
        @SuppressLint("BadHostnameVerifier")
        public boolean verify(String hostname, SSLSession session) {
            return true;
        }
    };

    private Button btnPost;
    private Button btnGet;
    private Button btnToken;

    private CheckBox checkSecure;

    private EditText editIP;
    private EditText editPort;
    private EditText editTopic;
    private EditText editSize;

    private String ip;
    private String port;
    private String topic;
    private String secure;
    private String token;

    private int size;

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
        btnPost = findViewById(R.id.btn_send);
        btnGet = findViewById(R.id.btn_receive);
        btnToken = findViewById(R.id.btn_token);

        checkSecure = findViewById(R.id.secureBox);

        editIP = findViewById(R.id.editIP);
        editPort = findViewById(R.id.editPort);
        editTopic = findViewById(R.id.editTopic);
        editSize = findViewById(R.id.editSize);

        btnPost.setOnClickListener(this);
        btnGet.setOnClickListener(this);
        btnToken.setOnClickListener(this);

        postQueue = Volley.newRequestQueue(this);
        getQueue = Volley.newRequestQueue(this);
        consumerQueue = Volley.newRequestQueue(this);

        generateConfigFile();

        readConfigFile();

        if (!(ip.isEmpty() || port.isEmpty() || topic.isEmpty())) {
            makeConsumer("testConsumer", "latest");
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            makeSubscription("testConsumer", topic);
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    @SuppressLint("NonConstantResourceId")
    @Override
    public void onClick(View v) {
        writeConfigFile();

        switch (v.getId()) {

            case R.id.btn_send:
                makePOST(generateRandomMessage(size), topic);
                break;
            case R.id.btn_token:
                getAccessToken(ip);
                break;
            default:
                break;
        }
    }

    private void generateConfigFile() {
        String path = getFilesDir() + "/config.json";

        File settingsFile = new File(path);

        if (!settingsFile.exists()) {
            JSONObject settingsJSON = new JSONObject();
            try {
                settingsJSON.put("ip", "");
                settingsJSON.put("port", "");
                settingsJSON.put("topic", "test");
                settingsJSON.put("size", "1");
                settingsJSON.put("secure", "false");


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

            ip = configJSON.getString("ip");
            port = configJSON.getString("port");
            topic = configJSON.getString("topic");
            size = Integer.parseInt(configJSON.getString("size"));
            secure = configJSON.getString("secure");

            editIP.setText(ip);
            editPort.setText(port);
            editTopic.setText(topic);
            editSize.setText(String.valueOf(size));

            checkSecure.setChecked(secure.equals("true"));

        } catch (IOException | JSONException e) {
            e.printStackTrace();
        }
    }

    private void writeConfigFile() {
        String path = getFilesDir() + "/config.json";

        ip = editIP.getText().toString();
        port = editPort.getText().toString();
        topic = editTopic.getText().toString();
        size = Integer.parseInt(editSize.getText().toString());
        secure = (checkSecure.isChecked() ? "true" : "false");

        JSONObject configJSON = new JSONObject();

        try {
            configJSON.put("ip", ip);
            configJSON.put("port", port);
            configJSON.put("topic", topic);
            configJSON.put("size", String.valueOf(size));
            configJSON.put("secure", secure);

            FileWriter writer = new FileWriter(path);
            writer.write(configJSON.toString());
            writer.flush();
        } catch (JSONException | IOException e) {
            e.printStackTrace();
        }
    }

    private void makePOST(String message, String topic) {
        String URL = "http://" + ip + ":" + port + "/topics/" + topic;
        StringRequest stringRequest;

        if (secure.equals("true")) {
            stringRequest = new StringRequest(Request.Method.POST, URL, new Response.Listener<String>() {
                @Override
                public void onResponse(String response) {
                    //Log.i("makePOST", "onResponse: " + response);
                }
            }, new Response.ErrorListener() {
                @Override
                public void onErrorResponse(VolleyError error) {
                    Log.w("makePOST", "onErrorResponse: " + error.getMessage());
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
                    //Log.i("makePOST", "onResponse: " + response);
                }
            }, new Response.ErrorListener() {
                @Override
                public void onErrorResponse(VolleyError error) {
                    Log.w("makePOST", "onErrorResponse: " + error.getMessage());
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

    private void makeConsumer(String consumerName, String autoOffsetReset) {
        String URL = "http://" + ip + ":" + port + "/consumers/" + consumerName;
        String data = "{\"name\": \"" + consumerName + "\", \"format\": \"json\", \"auto.offset.reset\": \"" + autoOffsetReset + "\", \"consumer.request.timeout.ms\": 10}";
        StringRequest stringRequest;

        if (secure.equals("true")) {
            stringRequest = new StringRequest(Request.Method.POST, URL, new Response.Listener<String>() {
                @Override
                public void onResponse(String response) {
                    Log.i("makeConsumer", "onResponse: " + response);
                }
            }, new Response.ErrorListener() {
                @Override
                public void onErrorResponse(VolleyError error) {
                    Log.w("makeConsumer", "onErrorResponse: " + error.getMessage());
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
                    Log.w("makeConsumer", "onErrorResponse: " + error.getMessage());
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

    private void makeSubscription(String consumerName, String topic) {
        String URL = "http://" + ip + ":" + port + "/consumers/" + consumerName + "/instances/" + consumerName + "/subscription";
        String data = "{\"topics\":[\"" + topic + "\"]}";
        StringRequest stringRequest;

        if (secure.equals("true")) {
            stringRequest = new StringRequest(Request.Method.POST, URL, new Response.Listener<String>() {
                @Override
                public void onResponse(String response) {
                    Log.i("makeSubscription", "onResponse: " + response);
                }
            }, new Response.ErrorListener() {
                @Override
                public void onErrorResponse(VolleyError error) {
                    Log.w("makeSubscription", "onErrorResponse: " + error.getMessage());
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
                    Log.w("makeSubscription", "onErrorResponse: " + error.getMessage());
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

    private void getAccessToken(String ip) {
        HttpURLConnection connection;
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
            url = new URL("https://" + ip + ":8443/auth/realms/faster/protocol/openid-connect/token");

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