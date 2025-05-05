-- CreateTable
CREATE TABLE "UserLogin" (
    "id" SERIAL NOT NULL,
    "login_time" TIMESTAMP(6) NOT NULL,
    "uid" TEXT NOT NULL,
    "email" TEXT,
    "mobile" TEXT,
    "name" TEXT,
    "id_type" TEXT,
    "id_no" TEXT,
    "ebid" TEXT,
    "eid" TEXT,
    "salary" INTEGER,

    CONSTRAINT "UserLogin_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "UserLogin_login_time_idx" ON "UserLogin"("login_time");

-- CreateIndex
CREATE INDEX "UserLogin_uid_idx" ON "UserLogin"("uid");

-- CreateIndex
CREATE INDEX "UserLogin_email_idx" ON "UserLogin"("email");
