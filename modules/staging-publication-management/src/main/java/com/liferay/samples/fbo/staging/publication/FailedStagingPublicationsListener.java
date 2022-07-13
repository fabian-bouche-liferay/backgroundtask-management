package com.liferay.samples.fbo.staging.publication;

import com.liferay.exportimport.kernel.background.task.BackgroundTaskExecutorNames;
import com.liferay.portal.background.task.model.BackgroundTask;
import com.liferay.portal.kernel.backgroundtask.BackgroundTaskManager;
import com.liferay.portal.kernel.backgroundtask.constants.BackgroundTaskConstants;
import com.liferay.portal.kernel.exception.ModelListenerException;
import com.liferay.portal.kernel.exception.PortalException;
import com.liferay.portal.kernel.model.BaseModelListener;
import com.liferay.portal.kernel.model.ModelListener;
import com.liferay.portal.kernel.service.GroupLocalService;
import com.liferay.portal.kernel.service.ServiceContext;
import com.liferay.portal.kernel.transaction.TransactionCommitCallbackUtil;
import com.liferay.samples.fbo.staging.publication.constants.FailedStagingPublicationsConstants;

import java.io.Serializable;
import java.util.Map;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(
	    immediate = true,
	    service = ModelListener.class
	)
public class FailedStagingPublicationsListener extends BaseModelListener<BackgroundTask> {

	private final static Logger LOG = LoggerFactory.getLogger(FailedStagingPublicationsListener.class);
	
	private final static int MAX_REEXECUTION_ATTEMPTS = 5;

	@Override
	public void onAfterUpdate(BackgroundTask originalBackgroundTask, BackgroundTask backgroundTask) throws ModelListenerException {

		if(FailedStagingPublicationsConstants.STAGING_EXECUTOR_CLASSNAME.equals(backgroundTask.getTaskExecutorClassName())) {
			if(BackgroundTaskConstants.STATUS_FAILED == backgroundTask.getStatus()) {
				if(backgroundTask.getStatusMessage().contains(FailedStagingPublicationsConstants.LOCK_ACQUISITION_EXCEPTION)) {
					long groupId = backgroundTask.getGroupId();
					String groupFriendlyURL;
					try {
						groupFriendlyURL = _groupLocalService.getGroup(groupId).getFriendlyURL();
						LOG.warn("Failed to execute staging publication background task for site {} because of a LockAcquisitionException", groupFriendlyURL);
						
						String backgroundTaskName = "Reexecution of " + backgroundTask.getName();
						long userId = backgroundTask.getUserId();
						Map<String, Serializable> taskContextMap = backgroundTask.getTaskContextMap();
						long reexecutionAttempt;
						if(taskContextMap.get(FailedStagingPublicationsConstants.REEXECUTION_ATTEMPT) == null) {
							reexecutionAttempt = 1;
						} else {
							reexecutionAttempt = (long) taskContextMap.get(FailedStagingPublicationsConstants.REEXECUTION_ATTEMPT);
						}
						taskContextMap.put(FailedStagingPublicationsConstants.REEXECUTION_ATTEMPT, reexecutionAttempt);
						
						if(reexecutionAttempt <= MAX_REEXECUTION_ATTEMPTS) {
							
							TransactionCommitCallbackUtil.registerCallback(() -> {

								com.liferay.portal.kernel.backgroundtask.BackgroundTask newBackgroundTask = _backgroundTaskManager.addBackgroundTask(
										userId,
										groupId,
										backgroundTaskName,
										BackgroundTaskExecutorNames.LAYOUT_STAGING_BACKGROUND_TASK_EXECUTOR,
										taskContextMap,
										new ServiceContext());

								LOG.warn("Trying again with a new background task {} (Attempt #{})", newBackgroundTask.getBackgroundTaskId(), reexecutionAttempt);

								return null;
							});

						} else {
							LOG.error("Giving up because there have already been {} failed attempts", MAX_REEXECUTION_ATTEMPTS);
						}
						
					} catch (PortalException e) {
						LOG.error("Failed to find group with groupId {}", groupId);
					}
				}
			}
		}
		
		super.onAfterUpdate(originalBackgroundTask, backgroundTask);
	}
	
	@Reference
	private BackgroundTaskManager _backgroundTaskManager;
	
	@Reference
	private GroupLocalService _groupLocalService;
	
}
