package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.TextView;

public class SimpleDhtActivity extends Activity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);







        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));



        findViewById(R.id.button1).setOnClickListener(
                new View.OnClickListener() {

                    private Uri buildUri(String scheme, String authority) {
                        Uri.Builder uriBuilder = new Uri.Builder();
                        uriBuilder.authority(authority);
                        uriBuilder.scheme(scheme);
                        return uriBuilder.build();
                    }

                    @Override
                    public void onClick(View view) {
                        Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");


                        Cursor resultCursor = getContentResolver().query(mUri, null,
                                "@", null, null);

                        int keyIndex = resultCursor.getColumnIndex("key");
                        int valueIndex = resultCursor.getColumnIndex("value");
                        TextView tv  = (TextView) findViewById(R.id.textView1);
                        tv.setText("");
                        for (resultCursor.moveToFirst(); !resultCursor.isAfterLast(); resultCursor.moveToNext()) {
                            // do what you need with the cursor here
                            String k = resultCursor.getString(keyIndex);
                            String v = resultCursor.getString(valueIndex);
                            Log.v("Q", k + "-" +v);
                            tv.append(k + ": " + v + "\n");

                        }
                        resultCursor.close();

                    }
                }
        );

        findViewById(R.id.button2).setOnClickListener(
                new View.OnClickListener() {

                    private Uri buildUri(String scheme, String authority) {
                        Uri.Builder uriBuilder = new Uri.Builder();
                        uriBuilder.authority(authority);
                        uriBuilder.scheme(scheme);
                        return uriBuilder.build();
                    }

                    @Override
                    public void onClick(View view) {
                        Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

                        Cursor resultCursor = getContentResolver().query(mUri, null,
                                "*", null, null);

                        int keyIndex = resultCursor.getColumnIndex("key");
                        int valueIndex = resultCursor.getColumnIndex("value");
                        TextView tv  = (TextView) findViewById(R.id.textView1);
                        tv.setText("");
                        for (resultCursor.moveToFirst(); !resultCursor.isAfterLast(); resultCursor.moveToNext()) {
                            // do what you need with the cursor here
                            String k = resultCursor.getString(keyIndex);
                            String v = resultCursor.getString(valueIndex);
                            Log.v("Q", k + "-" +v);
                            tv.append(k + ": " + v + "\n");

                        }
                        resultCursor.close();

                    }
                }
        );



    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

}
