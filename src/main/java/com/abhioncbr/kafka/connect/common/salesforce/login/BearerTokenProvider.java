package com.abhioncbr.kafka.connect.common.salesforce.login;

import java.net.URL;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Container for BayeuxParameters and the bearerToken.
 * Calls BayeuxParameters supplier in re-authentication scenarios.
 *
 * @author pbn-sfdc
 */
public class BearerTokenProvider implements Function<Boolean, String> {

    private Supplier<BayeuxParameters> sessionSupplier;
    private String bearerToken;

    public BearerTokenProvider(Supplier<BayeuxParameters> sessionSupplier) {
        this.sessionSupplier = sessionSupplier;
    }

    public BayeuxParameters login() {
        BayeuxParameters parameters = sessionSupplier.get();
        bearerToken = parameters.bearerToken();
        return parameters;
    }

    @Override
    public String apply(Boolean reAuth) {
        if (reAuth) {
            try {
                bearerToken = sessionSupplier.get().bearerToken();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return bearerToken;
    }

    public static BearerTokenProvider getToken(String customURL, String username, String password){
        BearerTokenProvider tokenProvider = new BearerTokenProvider(() -> {
            try {
                return LoginHelper.login(new URL(customURL), username, password);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return tokenProvider;
    }
}
