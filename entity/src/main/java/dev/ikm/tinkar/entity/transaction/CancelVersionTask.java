/*
 * Copyright © 2015 Integrated Knowledge Management (support@ikm.dev)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.ikm.tinkar.entity.transaction;

import dev.ikm.tinkar.entity.*;
import dev.ikm.tinkar.terms.State;

/**
 * For canceling a single version. If the version is part of a shared transaction, then
 * it will be removed from that transaction, and added to a new single version transaction.
 */
public class CancelVersionTask extends TransactionVersionTask {
    public CancelVersionTask(ConceptVersionRecord version) {
        super(version);
    }

    public CancelVersionTask(PatternVersionRecord version) {
        super(version);
    }

    public CancelVersionTask(SemanticVersionRecord version) {
        super(version);
    }

    public CancelVersionTask(StampVersionRecord version) {
        super(version);
    }

    protected String getTitleString() {
        return "Canceling transaction: ";
    }

    protected void performTransactionAction(Transaction transactionForAction) {
        transactionForAction.cancel();
    }

    @Override
    protected State getStateForVersion(EntityVersion version) {
        return State.CANCELED;
    }

    protected long getTime() {
        return Long.MIN_VALUE;
    }
}
