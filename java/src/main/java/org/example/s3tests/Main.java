package org.example.s3tests;

import java.io.PrintWriter;

import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;

public class Main {
    public static void main(String[] args) {

        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
        .selectors(DiscoverySelectors.selectPackage("org.example.test"))
        .build();

        Launcher launcher = LauncherFactory.create();

        System.out.println("Test Start!");
        SummaryGeneratingListener listener = new SummaryGeneratingListener();
        launcher.registerTestExecutionListeners(listener);
        launcher.execute(request);
        listener.getSummary().printTo(new PrintWriter(System.out));
        printReport(listener.getSummary());
    }

    private static void printReport(TestExecutionSummary summary) {
        System.out.println(
                "\n------------------------------------------" +
                "\nTests started: " + summary.getTestsStartedCount() +
                "\nTests failed: " + summary.getTestsFailedCount() +
                "\nTests succeeded: " + summary.getTestsSucceededCount() +
                "\n------------------------------------------"
        );

        if(summary.getTestsFailedCount() > 0) {
            for(TestExecutionSummary.Failure f: summary.getFailures()){
                System.out.println(f.getTestIdentifier().getSource() + "\n\nException " + f.getException());
            }
        }
    }
}
