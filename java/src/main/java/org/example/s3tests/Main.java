/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version
* 3 of the License. See LICENSE for details
*
* 본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
* KSAN 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
* KSAN 개발팀은 사전 공지, 허락, 동의 없이 KSAN 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.
*/
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

		LauncherDiscoveryRequest request = null;

		if (args.length == 1) {
			
		} else if (args.length == 2) {
			String className = args[1];
			System.out.println("Class Test " + className);

			request = LauncherDiscoveryRequestBuilder.request()
					.selectors(DiscoverySelectors.selectClass(getTestPackageName(className, null)))
					.build();
		} else if (args.length == 3) {
			String className = args[1];
			String methodName = args[2];
			System.out.printf("Method Test %s.%s\n", className, methodName);

			request = LauncherDiscoveryRequestBuilder.request()
					.selectors(DiscoverySelectors.selectMethod(getTestPackageName(className, methodName)))
					.build();
		} else {
			System.out.println("Full Test");
			request = LauncherDiscoveryRequestBuilder.request()
					.selectors(DiscoverySelectors.selectPackage(getTestPackageName(null,null)))
					.build();
		}

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
						"\n------------------------------------------");

		if (summary.getTestsFailedCount() > 0) {
			for (TestExecutionSummary.Failure f : summary.getFailures()) {
				System.out.println(f.getTestIdentifier().getSource() + "\n\nException " + f.getException());
			}
		}
	}

	private static String packageName = "org.example.test";

	private static String getTestPackageName(String className, String methodName) {
		if (className == null)
			return packageName;
		else if (methodName == null)
			return String.format("%s.%s", packageName, className);
		else
			return String.format("%s.%s#%s", packageName, className, methodName);
	}
}
